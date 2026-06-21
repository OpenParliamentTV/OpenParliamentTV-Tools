"""Parliament-agnostic workflow orchestrator.

Drives the shared stages (merge / NEL / align / NER / publish) and delegates
to ``WorkflowHooks`` for the genuinely parliament-specific pieces (scrapers,
parsers, merge call shape, align call shape). Each parliament's
``workflow.py`` defines the hooks and calls ``run_workflow``.
"""

import argparse
import json
import logging
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Optional

from optv.shared.nel import get_nel_data, link_entities_from_file
from optv.shared.ner import extract_entities_from_file
from optv.shared.publish import (
    carry_forward_documents,
    carry_forward_enrichments,
    carry_forward_wids,
    data_signature,
    is_demotion,
    strip_legacy_textbody_ids,
)
from optv.shared.session_status import SessionStatus
from optv.shared.validators import validate_stage2

logger = logging.getLogger(__name__)


@dataclass
class WorkflowHooks:
    """Parliament-specific adapters used by ``run_workflow``.

    Signatures:
      download_originals(config, args) -> None
          Body of ``--download-original``. Optional.
      parse_originals(config, args) -> None
          Always called after download (mtime-driven). Optional.
      merge_session_to_file(config, session, args) -> Path
          Produce the merged cache file for one session and return its path.
      align_session_to_file(config, session, args) -> Path
          Produce the aligned cache file for one session and return its path.
      session_in_scope(args, session) -> bool
          Optional; defaults to ``startswith(str(args.period))`` + ``--limit-session``.
    """
    parliament_id: str
    download_originals: Optional[Callable] = None
    parse_originals: Optional[Callable] = None
    merge_session_to_file: Optional[Callable] = None
    align_session_to_file: Optional[Callable] = None
    session_in_scope: Optional[Callable] = None


# ---- shared internal helpers (kept private; identical across parliaments) ----

def _run_stage2_validation(args, session: str, doc: dict) -> None:
    if not getattr(args, "validate", True):
        return
    findings = validate_stage2(doc, schema="full", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    warnings_ = [f for f in findings if f["severity"] == "warning"]
    if errors:
        logger.error(
            f"[{session}] stage2 validation: {len(errors)} error(s), "
            f"{len(warnings_)} warning(s) — publish NOT blocked"
        )
        for f in errors[:10]:
            logger.error(f"  [{session}] {f['rule']} @ {f['path']}: {f['message'][:240]}")
        if len(errors) > 10:
            logger.error(f"  [{session}] ... {len(errors) - 10} more error(s) suppressed")
    elif warnings_:
        logger.info(f"[{session}] stage2 validation: {len(warnings_)} warning(s)")


def _publish_as_processed(config, args, session: str, filepath: Path) -> Path:
    """Publish a produced cache file into ``processed/``.

    Non-destructive: refuses to demote a richer published session (dropping
    alignment/NER/documents) and carries already-published entity links,
    per-speech enrichments and document links forward, so a publish can only
    ever add wids / agendaItem types / debug.confidence / documents values,
    never remove them. Strict on corrupt published JSON — see
    optv/shared/publish.py for rationale.
    """
    processed_file = config.file(session, 'processed', create=True)
    published_data = {'data': []}
    if processed_file.exists():
        published_data = json.loads(processed_file.read_text())
    new_doc = json.loads(filepath.read_text())
    strip_legacy_textbody_ids(new_doc['data'])

    if is_demotion(new_doc['data'], published_data['data']):
        logger.warning(f"Not publishing {session} from {filepath.name}: "
                       f"would drop transcript/alignment/NER already in processed/")
        return processed_file

    carried = carry_forward_wids(new_doc['data'], published_data['data'])
    if carried:
        logger.warning(f"Carried {carried} already-published wid(s) forward "
                       f"while publishing {session} from {filepath.name}")
    enriched = carry_forward_enrichments(new_doc['data'], published_data['data'])
    if enriched:
        logger.warning(f"Carried {enriched} already-published enrichment field(s) "
                       f"forward while publishing {session} from {filepath.name}")
    docs_carried = carry_forward_documents(new_doc['data'], published_data['data'])
    if docs_carried:
        logger.warning(f"Carried documents for {docs_carried} speech(es) forward "
                       f"while publishing {session} from {filepath.name}")

    if data_signature(published_data['data']) != data_signature(new_doc['data']):
        logger.warning(f"Publishing {session} from {filepath.name}")
        _run_stage2_validation(args, session, new_doc)
        with open(processed_file, 'w') as f:
            json.dump(new_doc, f, indent=2, ensure_ascii=False)
    return processed_file


def _enrichment_source(config, session: str) -> Path:
    """Richest file to re-run an in-place enrichment (NEL / NER) over.

    ``processed/`` is the published high-water mark: the demotion guard in
    publish.py keeps it at least as rich as any local stage cache, and it is
    the only state that travels between machines via git. So prefer it, and
    fall back to the freshest cache (ner → aligned → merged) only before the
    first publish exists.

    Sourcing from the cache instead let a stale media-only ``aligned`` stub
    (produced when proceedings were briefly unavailable) shadow a fully
    transcribed published session: NER then ran over text-less input and the
    publish guard correctly refused the demoted result, so the stage silently
    no-op'd on every run while ``processed/`` already held the real transcript.
    """
    processed_file = config.file(session, 'processed')
    if processed_file.exists():
        return processed_file
    for stage in ('ner', 'aligned', 'merged'):
        stage_file = config.file(session, stage)
        if stage_file.exists():
            return stage_file
    return config.file(session, 'merged')


def _enrichment_is_current(source_file: Path, stage: str,
                           upstream: tuple = ('merge', 'align')) -> bool:
    """True if ``stage`` (``nel`` / ``ner``) has already processed the current
    state of ``source_file``.

    Reads the file's own ``meta.processing`` -- these timestamps travel inside
    the published JSON, so the gate is identical on every machine, unlike a
    comparison of local cache mtimes (the old ``is_newer(aligned, ner)`` gate,
    which let a leftover cache file on one machine silently suppress a stage
    that had never actually published). The stage re-runs whenever it has no
    recorded pass yet, or an upstream stage advanced past its last pass (e.g. a
    re-merge added speeches mid-broadcast).

    Empty or fully-unmatched sessions still settle after one pass: the
    enrichment writers always stamp their timestamp on first run even when
    nothing changed, so the gate flips to current and the loop stops. An
    entity-registry refresh (new wid for a previously-unmatchable label) is
    not auto-detected; use --force to re-propagate.
    """
    try:
        with open(source_file) as f:
            doc = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    proc = doc.get('meta', {}).get('processing', {})
    stage_ts = proc.get(stage)
    if not stage_ts:
        return False
    upstream_ts = max(
        (proc[k] for k in upstream if k in proc),
        default=None,
    )
    return upstream_ts is None or stage_ts >= upstream_ts


# NEL only mutates people[] (wids/factions), which only a *merge* can change --
# align (sentence timing) and ner never touch it. So merge is NEL's only
# upstream. Counting align here (as the default ('merge', 'align') does) re-ran
# NEL on every cron for any aligned session: the workflow always runs nel before
# align, so the align timestamp is permanently newer than the nel timestamp, and
# the gate never settled. It was write-guarded (no commit), but it redid the
# linking work and logged a spurious "Linking entities for ..." every run.
_NEL_UPSTREAM = ('merge',)


def _nel_is_current(source_file: Path) -> bool:
    """Back-compat wrapper -- see ``_enrichment_is_current``."""
    return _enrichment_is_current(source_file, 'nel', upstream=_NEL_UPSTREAM)


def _default_session_in_scope(args, session: str) -> bool:
    if args.limit_to_period and not session.startswith(str(args.period)):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


# ---- machine-local per-period stage state (watermarks + NEL dump marker) ----
#
# Optimization only: lets a stage skip its (JSON-parsing) worklist scan when no
# in-scope input file has changed since the last all-clear pass, so a run that
# processes nothing collapses each stage to one line instead of a multi-second
# scan. The state file lives under cache/ (gitignored, machine-local) so it
# never travels between machines; git pull stamps any updated file with the
# local current mtime, so a change another machine pushed always reads as
# "newer than the watermark" here and forces a scan. The authoritative
# per-session gates (is_newer / meta.processing stamps) still decide real work
# whenever a scan runs -- the watermark only decides *whether* to scan.
#
# Keyed by scope (the period when --limit-to-period is set, else "all") so a
# period-21 run can never mark period-20 as up to date.

def _state_path(config) -> Path:
    return config.dir('cache', create=True) / '.stage-state.json'


def _load_state(config) -> dict:
    try:
        with open(_state_path(config)) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_state(config, state: dict) -> None:
    try:
        with open(_state_path(config), 'w') as f:
            json.dump(state, f, indent=2, sort_keys=True)
    except OSError as e:
        logger.debug(f"Could not persist stage state: {type(e).__name__}: {e}")


def _scope_key(args) -> str:
    return str(args.period) if getattr(args, 'limit_to_period', False) else 'all'


def _get_state(state: dict, scope_key: str, key: str):
    return state.get(scope_key, {}).get(key)


def _set_state(state: dict, scope_key: str, key: str, value) -> None:
    state.setdefault(scope_key, {})[key] = value


def _max_inscope_mtime(config, args, in_scope, stages) -> float:
    """Newest mtime among the in-scope ``stages`` files (stat only, no parse)."""
    newest = 0.0
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        for stage in stages:
            try:
                m = config.file(session, stage).stat().st_mtime
            except OSError:
                continue
            if m > newest:
                newest = m
    return newest


def _watermark_skips(config, args, in_scope, state, scope_key, stage, inputs):
    """(skip, current_mtime) for ``stage``: skip the scan iff no in-scope input
    file is newer than the recorded all-clear watermark. ``--force`` never
    skips. Returns the freshly-computed mtime so the caller can record it after
    an all-clear scan."""
    current = _max_inscope_mtime(config, args, in_scope, inputs)
    if getattr(args, 'force', False):
        return False, current
    recorded = _get_state(state, scope_key, stage)
    return (recorded is not None and current <= recorded), current


# ---- stage runners ----

def _run_merge_stage(config, args, hooks: WorkflowHooks, in_scope, publish,
                     state, scope_key) -> None:
    skip, current_mtime = _watermark_skips(
        config, args, in_scope, state, scope_key, 'merge', ['media', 'proceedings'])
    if skip:
        logger.info("Merge: all in-scope sessions up to date, nothing to merge")
        return
    force = bool(getattr(args, 'force', False))
    todo = [s for s in config.sessions()
            if in_scope(args, s)
            and (force
                 or config.is_newer(s, 'media', 'merged')
                 or config.is_newer(s, 'proceedings', 'merged'))]
    if not todo:
        logger.info("Merge: all in-scope sessions up to date, nothing to merge")
        _set_state(state, scope_key, 'merge', current_mtime)
        return
    logger.info(
        f"Merging data from {config.dir('media')} and {config.dir('proceedings')} "
        f"into {config.dir('merged')} ({len(todo)} session(s))"
    )
    for session in todo:
        merged_file = hooks.merge_session_to_file(config, session, args)
        status = config.status(session)
        # Don't publish a bare merge over an aligned/NER'd published file.
        if SessionStatus.aligned in status or SessionStatus.ner in status:
            continue
        publish(session, merged_file)


def _run_update_nel_entities_stage(args, parliament_id: str) -> None:
    import urllib.request
    url = (getattr(args, "nel_entity_url", "") or "").strip()
    if not url:
        try:
            from optv.parliaments import load_manifest
            url = load_manifest(parliament_id).get("entity_dump_url", "")
        except (FileNotFoundError, ImportError) as e:
            logger.warning(f"Cannot read manifest entity_dump_url: {type(e).__name__}: {e}")
            url = ""
    if not url:
        # Supported mode, not an error: a parliament whose platform isn't set up
        # yet has no dump URL, so the committed entities.json (arriving via git)
        # is the source of truth. Part B's re-link trigger still fires when that
        # file changes (it hashes the on-disk dump, source-agnostic).
        logger.info(f"No entity-dump platform configured for {parliament_id} — "
                    f"using committed entities.json")
        return
    metadata_dir = args.data_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    target = metadata_dir / "entities.json"
    force = bool(getattr(args, "force", False))
    logger.info(f"Fetching NEL entity dump from {url}")
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            data = resp.read()
    except Exception as e:
        logger.warning(f"Could not fetch NEL entity dump from {url}: "
                       f"{type(e).__name__}: {e} — keeping existing entities.json")
        return
    # Sanity-check the fetched dump before trusting it. The platform is
    # authoritative, so a legitimate shrink (entity removed) must propagate --
    # we reject only a *transient glitch*: invalid JSON, empty, or an
    # implausible collapse vs the committed dump. --force overrides every guard.
    try:
        new_count = len(json.loads(data).get("data", []))
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"Fetched NEL entity dump is not valid JSON "
                       f"({type(e).__name__}) — keeping existing entities.json")
        return
    if new_count == 0 and not force:
        logger.warning("Fetched NEL entity dump is empty — keeping existing "
                       "entities.json (use --force to override)")
        return
    old_count = None
    if target.exists():
        try:
            old_count = len(json.loads(target.read_text()).get("data", []))
        except (OSError, json.JSONDecodeError):
            old_count = None
    if old_count and not force and new_count < old_count * 0.5:
        logger.warning(
            f"Fetched NEL entity dump collapsed implausibly ({new_count} entities "
            f"vs {old_count} committed) — keeping existing entities.json "
            f"(use --force to override)")
        return
    if target.exists() and target.read_bytes() == data:
        logger.info(f"NEL entity dump unchanged ({new_count} entities)")
        return
    target.write_bytes(data)
    persons, factions = get_nel_data(metadata_dir)
    logger.info(f"NEL entity dump updated: {len(data)} bytes, "
                f"{len(persons)} persons, {len(factions)} factions")


def _run_nel_stage(config, args, in_scope, publish, state, scope_key) -> None:
    nel_data_dir = config.dir('nel_data')
    if nel_data_dir is None or not nel_data_dir.is_dir():
        logger.error(f"Cannot do NEL - {nel_data_dir} does not exist")
        return
    if not (nel_data_dir / 'entities.json').exists():
        # Nothing to link against (e.g. platform not set up and no committed
        # dump) -- don't claim to link.
        logger.info("NEL link: no entities.json available, nothing to link")
        return
    force = bool(getattr(args, 'force', False))
    skip, current_mtime = _watermark_skips(
        config, args, in_scope, state, scope_key, 'nel', ['processed'])
    if skip:
        logger.info("NEL link: all sessions current, nothing to link")
        return
    persons, factions = get_nel_data(nel_data_dir)
    todo = []
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        source_file = _enrichment_source(config, session)
        if not source_file.exists():
            continue
        if not force and _enrichment_is_current(source_file, 'nel', upstream=_NEL_UPSTREAM):
            continue
        todo.append((session, source_file))
    if not todo:
        logger.info("NEL link: all sessions current, nothing to link")
        _set_state(state, scope_key, 'nel', current_mtime)
        return
    logger.info(f"Linking entities with wikidata IDs ({len(todo)} session(s))")
    for session, source_file in todo:
        logger.warning(f"Linking entities for {session} from {source_file.name}")
        link_entities_from_file(source_file, source_file, persons, factions)
        publish(session, source_file)


def _run_align_stage(config, args, hooks: WorkflowHooks, in_scope, publish,
                     state, scope_key) -> None:
    # The merged cache (align's only work source) is gitignored and only ever
    # produced locally, so its mtime reliably tracks "new content to align".
    skip, current_mtime = _watermark_skips(
        config, args, in_scope, state, scope_key, 'align', ['merged'])
    if skip:
        logger.info("Time-alignment: all sessions current, nothing to align")
        return
    force = bool(getattr(args, 'force', False))
    todo = []
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        if not force:
            status = config.status(session)
            # Media-only session (no merged proceedings transcript): align has
            # no sentences to feed aeneas; it would only rewrite an empty
            # aligned cache and re-publish. A no_text session also never gains
            # the `aligned` flag, so this is the only thing that stops a
            # cache-mtime bump from re-triggering the pass.
            if SessionStatus.no_text in status:
                continue
            if SessionStatus.aligned in status:
                continue
            if not config.is_newer(session, "merged", "aligned"):
                continue
        todo.append(session)
    if not todo:
        logger.info("Time-alignment: all sessions current, nothing to align")
        _set_state(state, scope_key, 'align', current_mtime)
        return
    logger.info(f"Updating time-alignment for merged files ({len(todo)} session(s))")
    for session in todo:
        logger.warning(f"Time-aligning {session}")
        try:
            aligned_file = hooks.align_session_to_file(config, session, args)
            publish(session, aligned_file)
        except Exception as e:
            logger.error(
                f"Alignment failed for session {session}: "
                f"{type(e).__name__}: {e} — continuing with next session"
            )


def _run_ner_stage(config, args, in_scope, publish, state, scope_key) -> None:
    skip, current_mtime = _watermark_skips(
        config, args, in_scope, state, scope_key, 'ner', ['processed'])
    if skip:
        logger.info("NER: all sessions current, nothing to extract")
        return
    force = bool(getattr(args, 'force', False))
    todo = []
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        if not force:
            status = config.status(session)
            if SessionStatus.no_text in status:
                # No merged proceedings transcript to run NER over (media-only
                # session) — same rationale as the align stage above.
                continue
        source_file = _enrichment_source(config, session)
        if not source_file.exists():
            continue
        # Gate on the file's own meta.processing.ner (travels via git), not on
        # SessionStatus / cache mtimes — those keyed off whichever stale local
        # cache happened to exist and silently skipped sessions whose published
        # file had no NER. See _enrichment_is_current / _enrichment_source.
        if not force and _enrichment_is_current(source_file, 'ner'):
            continue
        todo.append((session, source_file))
    if not todo:
        logger.info("NER: all sessions current, nothing to extract")
        _set_state(state, scope_key, 'ner', current_mtime)
        return
    logger.info(f"Updating NER for published sessions ({len(todo)} session(s))")
    for session, source_file in todo:
        logger.warning(f"Extracting Named Entities for {session} from {source_file.name}")
        ner_file = config.file(session, 'ner', create=True)
        extract_entities_from_file(source_file, ner_file, args)
        publish(session, ner_file)


# ---- public API ----

def run_workflow(config, args, hooks: WorkflowHooks) -> None:
    """Drive the parliament-agnostic stages, dispatching to hooks for the
    parliament-specific work."""
    publish = lambda s, f: _publish_as_processed(config, args, s, f)
    in_scope = hooks.session_in_scope or _default_session_in_scope
    state = _load_state(config)
    scope_key = _scope_key(args)

    if args.download_original and hooks.download_originals:
        hooks.download_originals(config, args)

    if hooks.parse_originals:
        hooks.parse_originals(config, args)

    if args.merge_speeches:
        _run_merge_stage(config, args, hooks, in_scope, publish, state, scope_key)

    if args.update_nel_entities:
        _run_update_nel_entities_stage(args, hooks.parliament_id)

    if args.link_entities:
        _run_nel_stage(config, args, in_scope, publish, state, scope_key)

    if args.align_sentences:
        _run_align_stage(config, args, hooks, in_scope, publish, state, scope_key)

    if args.extract_entities:
        _run_ner_stage(config, args, in_scope, publish, state, scope_key)

    _save_state(config, state)
    logger.info("Workflow done")


def build_common_argparser(*, description: str) -> argparse.ArgumentParser:
    """All the argparse flags that every parliament workflow shares.

    Parliament-specific flags (``--lang``, ``--inbox-dir``, ``--protokoll``,
    ...) are added by the per-parliament wrapper after this returns.

    ``--period`` is required for all parliaments; per-parliament wrapper
    scripts (e.g. DE-RP/update, DE/update) bake in the value when running.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("data_dir", type=str, nargs='?',
                        help="Data directory - mandatory")
    parser.add_argument("--debug", action="store_true", default=False,
                        help="Display debug messages")
    parser.add_argument("--period", type=int, required=True,
                        help="Period to fetch/consider (mandatory)")
    parser.add_argument("--force", action="store_true", default=False,
                        help="Force re-running stages even if outputs already exist")
    parser.add_argument("--cache-dir", type=str, default=None,
                        help="Cache directory (default DATADIR/cache)")
    parser.add_argument("--single-instance", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Exit if a lockfile is present (the process is already running)")
    parser.add_argument("--limit-to-period", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Limit work to sessions matching the period")
    parser.add_argument("--limit-session", default="",
                        help="Limit work to sessions matching regexp")
    parser.add_argument("--align-timeout", type=int, default=1200,
                        help="Wall-clock timeout (s) for aeneas per speech")
    parser.add_argument("--align-max-audio-seconds", type=int, default=2400,
                        help="Skip alignment if media duration exceeds this")
    parser.add_argument("--ner-api-endpoint", type=str, default="",
                        help="API endpoint URL for entityfishing server")
    parser.add_argument("--nel-entity-url", type=str, default="",
                        help="Override NEL entity dump URL (defaults to entity_dump_url from manifest.yaml)")
    # Shared across all parliaments (the Conductor passes all three to every
    # workflow). Defaults are filled from the manifest in run_main when unset,
    # so an explicit value (Conductor cron / update scripts) always wins.
    parser.add_argument("--lang", type=str, default=None,
                        help="Language override (default: manifest locale.aeneas_language)")
    parser.add_argument("--retry-count", type=int, default=None,
                        help="Max media download retries (default: manifest default_retry_count)")
    parser.add_argument("--retry-delay-max", type=float, default=None,
                        help="Max delay (s) between download retries (default: manifest default_retry_delay_max)")
    parser.add_argument("--validate", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Run Stage 2 schema+semantic validation on each publish (warning-only)")

    # Stage flags
    parser.add_argument("--download-original", action=argparse.BooleanOptionalAction,
                        default=False, help="Download original files")
    parser.add_argument("--merge-speeches", action=argparse.BooleanOptionalAction,
                        default=False, help="Merge media and proceeding files")
    parser.add_argument("--update-nel-entities", action=argparse.BooleanOptionalAction,
                        default=False, help="Download NEL entities from manifest URL")
    parser.add_argument("--link-entities", action=argparse.BooleanOptionalAction,
                        default=False, help="Link People/Faction entities to Wikidata IDs")
    parser.add_argument("--align-sentences", action=argparse.BooleanOptionalAction,
                        default=False, help="Do sentence-level audio alignment")
    parser.add_argument("--extract-entities", action=argparse.BooleanOptionalAction,
                        default=False, help="Run NER on aligned sessions")
    return parser


def _apply_manifest_defaults(args, parliament_id: str) -> None:
    """Fill ``--retry-count`` / ``--retry-delay-max`` / ``--lang`` from the
    manifest when the caller did not pass them explicitly."""
    try:
        from optv.parliaments import load_manifest
        manifest = load_manifest(parliament_id)
    except Exception as e:  # pragma: no cover - manifest always present in practice
        logger.warning(f"Cannot read manifest for {parliament_id}: {type(e).__name__}: {e}")
        manifest = {}
    if getattr(args, "retry_count", None) is None:
        args.retry_count = manifest.get("default_retry_count", 0)
    if getattr(args, "retry_delay_max", None) is None:
        args.retry_delay_max = manifest.get("default_retry_delay_max", 10.0)
    if getattr(args, "lang", None) is None:
        # aeneas_language is the canonical per-parliament language code; --lang
        # is the legacy alias the Conductor/align path still reads.
        args.lang = getattr(args, "aeneas_language", None)


def run_main(parliament_id: str, hooks: WorkflowHooks, *, description: str,
             add_arguments: Optional[Callable] = None, config_cls) -> None:
    """Shared entry point for every parliament's ``workflow.py``.

    Absorbs the boilerplate each ``main()`` repeated: build the common
    argparser (+ parliament-specific flags via ``add_arguments``), parse,
    set up logging, inject locale + manifest defaults, resolve data/cache dirs,
    acquire the single-instance lockfile, and drive ``run_workflow``.
    """
    parser = build_common_argparser(description=description)
    if add_arguments is not None:
        add_arguments(parser)
    args = parser.parse_args()
    if args.data_dir is None:
        parser.print_help()
        sys.exit(1)
    setup_logging(args.debug)
    inject_locale(args, parliament_id)
    _apply_manifest_defaults(args, parliament_id)
    args.data_dir = Path(args.data_dir)
    args.cache_dir = Path(args.cache_dir) if args.cache_dir else args.data_dir / "cache"
    with acquire_lockfile(args):
        config = config_cls(args.data_dir, cache_dir=args.cache_dir)
        run_workflow(config, args, hooks)


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s %(levelname)-8s %(name)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def inject_locale(args, parliament_id: str) -> None:
    """Inject the manifest's ``locale`` block onto ``args``.

    Sets ``spacy_model`` / ``aeneas_language`` / ``entityfishing_language``
    only when not already present, so explicit CLI overrides win.
    """
    from optv.parliaments import get_locale
    locale = get_locale(parliament_id)
    for key in ('spacy_model', 'aeneas_language', 'entityfishing_language'):
        if not getattr(args, key, None):
            setattr(args, key, locale[key])


@contextmanager
def acquire_lockfile(args) -> Iterator[None]:
    """Single-instance gate. No-op when ``args.single_instance`` is False."""
    if not args.single_instance:
        yield
        return
    lockfile = args.data_dir / "optv.lock"
    if lockfile.exists():
        logger.error(f"workflow already running as process {lockfile.read_text()} - exiting")
        sys.exit(1)
    lockfile.write_text(str(os.getpid()))
    try:
        yield
    finally:
        if lockfile.exists():
            lockfile.unlink()
