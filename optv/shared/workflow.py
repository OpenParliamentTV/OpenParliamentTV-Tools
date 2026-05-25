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
    carry_forward_enrichments,
    carry_forward_wids,
    data_signature,
    is_demotion,
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
    alignment/NER) and carries already-published entity links and per-speech
    enrichments forward, so a publish can only ever add wids / agendaItem
    types / debug.confidence values, never remove them. Strict on corrupt
    published JSON — see optv/shared/publish.py for rationale.
    """
    processed_file = config.file(session, 'processed', create=True)
    published_data = {'data': []}
    if processed_file.exists():
        published_data = json.loads(processed_file.read_text())
    new_doc = json.loads(filepath.read_text())

    if is_demotion(new_doc['data'], published_data['data']):
        logger.warning(f"Not publishing {session} from {filepath.name}: "
                       f"would drop alignment/NER already in processed/")
        return processed_file

    carried = carry_forward_wids(new_doc['data'], published_data['data'])
    if carried:
        logger.warning(f"Carried {carried} already-published wid(s) forward "
                       f"while publishing {session} from {filepath.name}")
    enriched = carry_forward_enrichments(new_doc['data'], published_data['data'])
    if enriched:
        logger.warning(f"Carried {enriched} already-published enrichment field(s) "
                       f"forward while publishing {session} from {filepath.name}")

    if data_signature(published_data['data']) != data_signature(new_doc['data']):
        logger.warning(f"Publishing {session} from {filepath.name}")
        _run_stage2_validation(args, session, new_doc)
        with open(processed_file, 'w') as f:
            json.dump(new_doc, f, indent=2, ensure_ascii=False)
    return processed_file


def _nel_source(config, session: str) -> Path:
    """Richest existing file to run NEL on, so re-linking never demotes.

    NEL only mutates ``people[]``, so linking an aligned/NER'd file preserves
    all timing and entity data. Fallback order: ner → aligned → processed →
    merged.
    """
    for stage in ('ner', 'aligned'):
        stage_file = config.file(session, stage)
        if stage_file.exists():
            return stage_file
    processed_file = config.file(session, 'processed')
    if processed_file.exists():
        return processed_file
    return config.file(session, 'merged')


def _default_session_in_scope(args, session: str) -> bool:
    if args.limit_to_period and not session.startswith(str(args.period)):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


# ---- stage runners ----

def _run_merge_stage(config, args, hooks: WorkflowHooks, in_scope, publish) -> None:
    logger.info(
        f"Merging data from {config.dir('media')} and {config.dir('proceedings')} "
        f"into {config.dir('merged')}"
    )
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        if not (config.is_newer(session, 'media', 'merged')
                or config.is_newer(session, 'proceedings', 'merged')
                or args.force):
            continue
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
        logger.warning("No NEL entity URL configured (no --nel-entity-url, "
                       "no entity_dump_url in manifest) - skipping")
        return
    metadata_dir = args.data_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    target = metadata_dir / "entities.json"
    logger.info(f"Downloading NEL entities from {url}")
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            data = resp.read()
        target.write_bytes(data)
        persons, factions = get_nel_data(metadata_dir)
        logger.info(f"NEL entities updated: {len(data)} bytes, "
                    f"{len(persons)} persons, {len(factions)} factions")
    except Exception as e:
        logger.warning(f"Could not download NEL entities from {url}: "
                       f"{type(e).__name__}: {e}")


def _run_nel_stage(config, args, in_scope, publish) -> None:
    nel_data_dir = config.dir('nel_data')
    if nel_data_dir is None or not nel_data_dir.is_dir():
        logger.error(f"Cannot do NEL - {nel_data_dir} does not exist")
        return
    persons, factions = get_nel_data(nel_data_dir)
    logger.info("Linking entities with wikidata IDs")
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        status = config.status(session)
        # Skip already-linked sessions; an entity-registry refresh needs --force
        # to re-propagate. Auto-detecting changes via entities.json mtime is
        # unreliable -- some cron paths re-curl the file every run, which would
        # otherwise cause every session to be re-linked + re-published every
        # run, producing timestamp-only commit churn downstream.
        if SessionStatus.linked in status and not args.force:
            continue
        source_file = _nel_source(config, session)
        if not source_file.exists():
            continue
        logger.warning(f"Linking entities for {session} from {source_file.name}")
        link_entities_from_file(source_file, source_file, persons, factions)
        publish(session, source_file)


def _run_align_stage(config, args, hooks: WorkflowHooks, in_scope, publish) -> None:
    logger.info("Updating time-alignment for merged files")
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        status = config.status(session)
        if SessionStatus.aligned in status and not args.force:
            logger.debug(f"Session {session} already aligned - not redoing")
            continue
        if not (config.is_newer(session, "merged", "aligned") or args.force):
            continue
        logger.warning(f"Time-aligning {session}")
        try:
            aligned_file = hooks.align_session_to_file(config, session, args)
            publish(session, aligned_file)
        except Exception as e:
            logger.error(
                f"Alignment failed for session {session}: "
                f"{type(e).__name__}: {e} — continuing with next session"
            )


def _run_ner_stage(config, args, in_scope, publish) -> None:
    logger.info("Updating NER for aligned files")
    for session in config.sessions():
        if not in_scope(args, session):
            continue
        status = config.status(session)
        if SessionStatus.ner in status and not args.force:
            logger.debug(f"Session {session} already NERed - not redoing")
            continue
        if not (config.is_newer(session, "aligned", "ner") or args.force):
            continue
        logger.warning(f"Extracting Named Entities for {session}")
        source_file = config.file(session, 'aligned')
        if not source_file.exists():
            # No aligned cache locally — use the published file as the
            # NER source instead (it is at least as rich as merged).
            source_file = config.file(session, 'processed')
        ner_file = config.file(session, 'ner', create=True)
        extract_entities_from_file(source_file, ner_file, args)
        publish(session, ner_file)


# ---- public API ----

def run_workflow(config, args, hooks: WorkflowHooks) -> None:
    """Drive the parliament-agnostic stages, dispatching to hooks for the
    parliament-specific work."""
    publish = lambda s, f: _publish_as_processed(config, args, s, f)
    in_scope = hooks.session_in_scope or _default_session_in_scope

    if args.download_original and hooks.download_originals:
        hooks.download_originals(config, args)

    if hooks.parse_originals:
        hooks.parse_originals(config, args)

    if args.merge_speeches:
        _run_merge_stage(config, args, hooks, in_scope, publish)

    if args.update_nel_entities:
        _run_update_nel_entities_stage(args, hooks.parliament_id)

    if args.link_entities:
        _run_nel_stage(config, args, in_scope, publish)

    if args.align_sentences:
        _run_align_stage(config, args, hooks, in_scope, publish)

    if args.extract_entities:
        _run_ner_stage(config, args, in_scope, publish)

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
