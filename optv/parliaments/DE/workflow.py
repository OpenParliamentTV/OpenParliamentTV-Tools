#! /usr/bin/env python3

# Update media files, proceeding files and merge them
import argparse
import atexit
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sys

# Allow relative imports if invoked as a script
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

from .common import Config, SessionStatus, data_signature

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

from .aligner.align_sentences import align_audiofile
from .ner.ner import extract_entities_from_file
from .nel.nel import link_entities_from_file, get_nel_data
from .scraper.update_media import update_media_directory_period, update_media_from_raw
from .scraper.fetch_proceedings import download_plenary_protocols
from .merger.merge_session import merge_session
from .parsers.proceedings2json import parse_proceedings_directory

def execute_workflow(args):
    config = Config(args.data_dir)

    def publish_as_processed(session: str, filepath: Path) -> Path:
        """Finalizing step - copy produced_files into processed
        This will be called after each step that produced a correct (even
        if incomplete) session file (merge, align, ner)
        """
        processed_file = config.file(session, 'processed', create=True)
        # Check that content is actually different. If not, do not save.
        # It happens when process such as nel/align is run again
        published_data = []
        if processed_file.exists():
            published_data = json.loads(processed_file.read_text())
        new_data = json.loads(filepath.read_text())
        # Compare actual data, ignoring metadata (with processing info)
        if data_signature(published_data['data']) != data_signature(new_data['data']):
            # Data is updated, copy new version
            logger.warning(f"Publishing {session} from {filepath.name}")
            shutil.copyfile(filepath, processed_file)
        return processed_file

    if args.download_original:
        logger.info(f"Downloading media and proceeding data for period {args.period}")
        # Download/parse new media data
        update_media_directory_period(args.period,
                                      config.dir('media'),
                                      force=args.force,
                                      save_raw_data=True,
                                      retry_count=args.retry_count)

        # Download new proceedings data
        download_plenary_protocols(config.dir('proceedings'),
                                   fullscan=False,
                                   period=args.period)

    # In any case, parse proceedings that need to
    parse_proceedings_directory(config.dir('proceedings'),
                                args)
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
            if config.is_newer(session, 'media', 'merged') or config.is_newer(session, 'proceedings', 'merged'):
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
                if SessionStatus.linked in status and not args.force:
                    continue
                merged_file = config.file(session, 'merged')
                logger.warning(f"Linking entities from {merged_file.name}")
                link_entities_from_file(merged_file,
                                        merged_file,
                                        persons, factions)
                publish_as_processed(session, merged_file)

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
                align_audiofile(merged_file, aligned_file, args.lang, args.cache_dir)
                publish_as_processed(session, aligned_file)

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
    parser.add_argument("--link-entities", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Link People/Faction entities")
    parser.add_argument("--extract-entities", action=argparse.BooleanOptionalAction,
                        default=False,
                        help="Do Entity extraction on aligned sessions (requires --align-sentences)")

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
