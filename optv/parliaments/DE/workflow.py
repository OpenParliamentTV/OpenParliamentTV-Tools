#! /usr/bin/env python3

# Update media files, proceeding files and merge them
import argparse
import atexit
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import sys

# Allow relative imports (for .common, .scraper, etc.) and absolute
# imports (for optv.shared.*) when invoked as a script.
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from .common import (Config, SessionStatus, data_signature,
                      is_demotion, carry_forward_wids)

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

from optv.shared.align import align_audiofile
from optv.shared.ner import extract_entities_from_file
from optv.shared.nel import link_entities_from_file, get_nel_data
from optv.shared.validators import validate_stage2

from .scraper.update_media import update_media_directory_period, update_media_from_raw
from .scraper.fetch_proceedings import download_plenary_protocols
from .scraper.fetch_parlamint import download_parlamint_period
from .merger.merge_session import merge_session
from .parsers.proceedings2json import parse_proceedings_directory
from .parsers.parlamint2json import parse_parlamint_directory

# Periods served by the ParlaMint-DE_beta corpus (Bundestag native TEI is
# only available from period 18 onwards).
PARLAMINT_PERIODS = {16, 17}

def execute_workflow(args):
    config = Config(args.data_dir)

    def run_stage2_validation(session: str, doc: dict) -> None:
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
            logger.info(
                f"[{session}] stage2 validation: {len(warnings_)} warning(s)"
            )

    def publish_as_processed(session: str, filepath: Path) -> Path:
        """Finalizing step - publish a produced file into processed/.

        Called after each step that produced a correct (even if incomplete)
        session file (merge, align, ner, nel).

        Non-destructive by design: refuses to demote a richer published
        session (dropping alignment/NER) and carries already-published entity
        links forward, so a publish can only ever add wids, never remove them.
        This keeps processed/ monotonic even when the pipeline is fed by an
        out-of-date cache (e.g. one produced on another machine).
        """
        processed_file = config.file(session, 'processed', create=True)
        published_data = { 'data': [] }
        if processed_file.exists():
            published_data = json.loads(processed_file.read_text())
        new_doc = json.loads(filepath.read_text())

        # Never replace a richer published session with a poorer one.
        if is_demotion(new_doc['data'], published_data['data']):
            logger.warning(f"Not publishing {session} from {filepath.name}: "
                           f"would drop alignment/NER already in processed/")
            return processed_file

        # Entity links are append-only across a publish.
        carried = carry_forward_wids(new_doc['data'], published_data['data'])
        if carried:
            logger.warning(f"Carried {carried} already-published wid(s) forward "
                           f"while publishing {session} from {filepath.name}")

        # Check that content is actually different. If not, do not save.
        # It happens when a process such as nel/align is run again.
        # Compare actual data, ignoring metadata (with processing info).
        if data_signature(published_data['data']) != data_signature(new_doc['data']):
            logger.warning(f"Publishing {session} from {filepath.name}")
            run_stage2_validation(session, new_doc)
            with open(processed_file, 'w') as f:
                json.dump(new_doc, f, indent=2, ensure_ascii=False)
        return processed_file

    def nel_source(session: str) -> Path:
        """Richest existing file to run NEL on, so re-linking never demotes.

        nel only mutates people[], so linking an aligned/NER'd file preserves
        all timing and entity data -- unlike linking the bare merged cache,
        which would demote processed/ on a forced re-run. Prefers the NER then
        aligned cache; the published file is at least as rich as the merged
        cache, so it is preferred over merged for an already-published session.
        """
        for stage in ('ner', 'aligned'):
            stage_file = config.file(session, stage)
            if stage_file.exists():
                return stage_file
        processed_file = config.file(session, 'processed')
        if processed_file.exists():
            return processed_file
        return config.file(session, 'merged')

    def nel_is_stale(session: str) -> bool:
        """True if entities.json changed since the NEL source was last linked.

        Lets an entity-registry refresh re-propagate on the next ordinary run
        without --force. Checks the file NEL actually rewrites, so a re-link
        advances its `nel` timestamp and the session is not flagged again.
        """
        nel_file = config.dir('nel_data') / 'entities.json'
        if not nel_file.exists():
            return False
        src = nel_source(session)
        if not src.exists():
            return True
        try:
            doc = json.loads(src.read_text())
            nel_ts = doc.get('meta', {}).get('processing', {}).get('nel')
            if not nel_ts:
                return True
            linked_at = datetime.fromisoformat(nel_ts).timestamp()
        except (json.JSONDecodeError, OSError, ValueError):
            return True
        return nel_file.stat().st_mtime > linked_at

    if args.download_original:
        logger.info(f"Downloading media and proceeding data for period {args.period}")
        # Download/parse new media data
        update_media_directory_period(args.period,
                                      config.dir('media'),
                                      force=args.force,
                                      save_raw_data=True,
                                      retry_count=args.retry_count)

        # Download new proceedings data
        if args.period in PARLAMINT_PERIODS:
            download_parlamint_period(args.period,
                                      config.dir('proceedings'),
                                      force=args.force)
        else:
            download_plenary_protocols(config.dir('proceedings'),
                                       fullscan=False,
                                       period=args.period)

    # In any case, parse proceedings that need to
    if args.period in PARLAMINT_PERIODS:
        parse_parlamint_directory(config.dir('proceedings'), args)
    else:
        parse_proceedings_directory(config.dir('proceedings'), args)
    # And also media
    update_media_from_raw(config.dir('media'))

    # Produce merged data
    if args.merge_speeches:
        logger.info(f"Merging data from {config.dir('media')} and {config.dir('proceedings')} into {config.dir('merged')}")
        for session in config.sessions():
            if args.limit_to_period and not session.startswith(str(args.period)):
                continue
            if args.limit_session and not re.match(args.limit_session, session):
                continue
            # Always redo the merge in case any source was updated
            if config.is_newer(session, 'media', 'merged') or config.is_newer(session, 'proceedings', 'merged') or args.force:
                merged_file = merge_session(session, config, args)
                status = config.status(session)
                # We want to directly publish this file if it did not exist
                # or if there is no data (time, ner) to lose doing it
                if (SessionStatus.aligned in status
                    or SessionStatus.ner in status):
                    continue
                # If we reach here, it is either that the processed file
                # was not present, or that it has no time/entity info, so
                # that we will lose nothing.
                publish_as_processed(session, merged_file)

    if args.update_nel_entities:
        import urllib.request
        url = (getattr(args, "nel_entity_url", "") or "").strip()
        if not url:
            try:
                from optv.parliaments import load_manifest
                url = load_manifest(Path(__file__).parent.name).get("entity_dump_url", "")
            except (FileNotFoundError, ImportError) as e:
                logger.warning(f"Cannot read manifest entity_dump_url: {type(e).__name__}: {e}")
                url = ""
        if not url:
            logger.warning("No NEL entity URL configured (no --nel-entity-url, no entity_dump_url in manifest) - skipping")
        else:
            metadata_dir = args.data_dir / "metadata"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            target = metadata_dir / "entities.json"
            logger.info(f"Downloading NEL entities from {url}")
            try:
                with urllib.request.urlopen(url, timeout=120) as resp:
                    data = resp.read()
                target.write_bytes(data)
                persons, factions = get_nel_data(metadata_dir)
                logger.info(f"NEL entities updated: {len(data)} bytes, {len(persons)} persons, {len(factions)} factions")
            except Exception as e:
                logger.warning(f"Could not download NEL entities from {url}: {type(e).__name__}: {e}")

    # Do entity linking for people and factions in merged files
    if args.link_entities:
        nel_data_dir = config.dir('nel_data')
        if nel_data_dir is None or not nel_data_dir.is_dir():
            logger.error(f"Cannot do NEL - {nel_data_dir} does not exist")
        else:
            persons, factions = get_nel_data(nel_data_dir)
            logger.info("Linking entities with wikidata IDs")
            for session in config.sessions():
                if args.limit_to_period and not session.startswith(str(args.period)):
                    continue
                if args.limit_session and not re.match(args.limit_session, session):
                    continue
                status = config.status(session)
                # Skip only when already linked AND entities.json has not
                # changed since -- otherwise an entity-registry refresh would
                # never reach already-processed sessions without --force.
                if (SessionStatus.linked in status
                        and not args.force
                        and not nel_is_stale(session)):
                    continue
                # Link the richest available file in place, so re-linking a
                # mature session preserves its alignment/NER instead of
                # demoting processed/ to a bare merge.
                source_file = nel_source(session)
                logger.debug(f"Linking entities from {source_file.name}")
                link_entities_from_file(source_file,
                                        source_file,
                                        persons, factions)
                publish_as_processed(session, source_file)

    # Time-align merged files - only when specified and only for processed files
    if args.align_sentences:
        logger.info("Updating time-alignment for merged files")
        for session in config.sessions():
            if args.limit_to_period and not session.startswith(str(args.period)):
                continue
            if args.limit_session and not re.match(args.limit_session, session):
                continue
            status = config.status(session)
            if SessionStatus.aligned in status and not args.force:
                # Already aligned. Do not overwrite.
                # If we want
                logger.debug(f"Session {session} already aligned - not redoing")
                continue
            if config.is_newer(session, "merged", "aligned") or args.force:
                logger.warning(f"Time-aligning {session}")
                merged_file = config.file(session, 'merged')
                aligned_file = config.file(session, 'aligned', create=True)
                try:
                    align_audiofile(merged_file, aligned_file, args.lang, args.cache_dir,
                                    timeout=args.align_timeout,
                                    max_audio_seconds=args.align_max_audio_seconds)
                    publish_as_processed(session, aligned_file)
                except Exception as e:
                    logger.error(f"Alignment failed for session {session}: {type(e).__name__}: {e} — continuing with next session")

    # NER aligned files
    if args.extract_entities:
        logger.info("Updating NER for aligned files")
        for session in config.sessions():
            if args.limit_to_period and not session.startswith(str(args.period)):
                continue
            if args.limit_session and not re.match(args.limit_session, session):
                continue
            status = config.status(session)
            if SessionStatus.ner in status and not args.force:
                # Already NERed. Do not overwrite.
                logger.debug(f"Session {session} already NERed - not redoing")
                continue
            if config.is_newer(session, "aligned", "ner") or args.force:
                logger.warning(f"Extracting Named Entities for {session}")
                source_file = config.file(session, 'aligned')
                if not source_file.exists():
                    # Maybe we do not have the cache for aligned
                    # data. Use the session file in this case.
                    source_file = config.file(session, 'processed')
                ner_file = config.file(session, 'ner', create=True)
                extract_entities_from_file(source_file, ner_file, args)
                publish_as_processed(session, ner_file)
    logger.info("Workflow done")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Update media files corresponding to proceeding XML files.")
    parser.add_argument("data_dir", type=str, nargs='?',
                        help="Data directory - mandatory")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    parser.add_argument("--period", type=int,
                        help="Period to fetch/consider (mandatory)")
    parser.add_argument("--retry-count", type=int,
                        dest="retry_count", default=0,
                        help="Max number of times to retry a media download")
    parser.add_argument("--align-timeout", type=int, default=1200,
                        help="Wall-clock timeout (s) for aeneas per speech (default: 1200)")
    parser.add_argument("--align-max-audio-seconds", type=int, default=2400,
                        help="Skip alignment if media duration exceeds this (default: 2400)")
    parser.add_argument("--force", dest="force", action="store_true",
                        default=False,
                        help="Force loading of data for a meeting even if the corresponding file already exists")
    parser.add_argument("--cache-dir", type=str, default=None,
                        help="Cache directory (default is DATADIR/cache")

    parser.add_argument("--single-instance", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Exits if a lockfile is present (the process is already running)")

    parser.add_argument("--lang", type=str, default="deu",
                        help="Language")

    parser.add_argument("--limit-to-period", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Limit time align and NER to specified period files")
    parser.add_argument("--limit-session", action="store",
                        default="",
                        help="Limit time align and NER to sessions matching regexp (eg 2001. for all 2001* sessions)")

    parser.add_argument("--ner-api-endpoint", type=str, default="",
                        help="API endpoint URL for entityfishing server")

    # Processing steps
    parser.add_argument("--download-original", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Download original files")
    parser.add_argument("--merge-speeches", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Merge media and proceeding files")
    parser.add_argument("--align-sentences", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Do the sentence alignment for downloaded sentences")
    parser.add_argument("--update-nel-entities", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Download NEL entities from OPTV server")
    parser.add_argument("--nel-entity-url", type=str, default="",
                        help="Override NEL entity dump URL (defaults to entity_dump_url from manifest.yaml)")
    parser.add_argument("--link-entities", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Link People/Faction entities")
    parser.add_argument("--extract-entities", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Do Entity extraction on aligned sessions (requires --align-sentences)")
    parser.add_argument("--validate", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Run Stage 2 schema+semantic validation on each publish (warning-only, does not block)")

    args = parser.parse_args()
    if args.data_dir is None or args.period is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel,
                        format='%(asctime)s %(levelname)-8s %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Inject locale config from manifest so optv.shared.{align,ner} can read
    # parliament-specific spaCy/aeneas/entityfishing settings off `args`.
    from optv.parliaments import get_locale
    locale = get_locale(Path(__file__).parent.name)
    if not getattr(args, "spacy_model", None):
        args.spacy_model = locale["spacy_model"]
    if not getattr(args, "entityfishing_language", None):
        args.entityfishing_language = locale["entityfishing_language"]

    args.data_dir = Path(args.data_dir)

    if args.single_instance:
        lockfile = args.data_dir / "optv.lock"
        # Checking for the presence of lock file
        if lockfile.exists():
            logger.error(f"workflow already running as process {lockfile.read_text()} - exiting")
            sys.exit(1)
        else:
            lockfile.write_text(str(os.getpid()))
            # Remove file on script exit
            atexit.register(lambda f: f.unlink(), lockfile)

    if args.cache_dir is None:
        args.cache_dir = args.data_dir / "cache"
    else:
        args.cache_dir = Path(args.cache_dir)

    execute_workflow(args)
