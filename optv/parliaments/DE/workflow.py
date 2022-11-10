#! /usr/bin/env python3

# Update media files, proceeding files and merge them
import argparse
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

from .common import Config, SessionStatus

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

from .aligner.align_sentences import align_audiofile
from .ner.ner import extract_entities_from_file
from .scraper.update_media import update_media_directory_period
from .scraper.fetch_proceedings import download_plenary_protocols
from .merger.merge_session import merge_files_or_dirs
from .parsers.proceedings2json import parse_proceedings_directory

def execute_workflow(args):
    config = Config(args.data_dir)

    def publish_as_processed(sessionfile_list: list[str, Path]):
        """Finalizing step - copy produced_files into processed
        This will be called after each step that produced a correct (even
        if incomplete) session file (merge, align, ner)
        """
        for session, path in sessionfile_list:
            processed_file = config.file(session, 'processed')
            shutil.copyfile(path, processed_file)

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

        # Update proceedings that need to be updated
        parse_proceedings_directory(config.dir('proceedings'),
                                    args)

    # FIXME: change testing logic - use config.status to check for stage in final file and execute appropriately

    # Produce merged data
    logger.info(f"Merging data from {config.dir('media')} and {config.dir('proceedings')} into {config.dir('merged')}")
    # Produce merged data into merged_dir
    merged_files = merge_files_or_dirs(config.dir('media'),
                                       config.dir('proceedings'),
                                       config.dir('merged'),
                                       args)
    publish_as_processed(merged_files)

    # Time-align merged files
    if args.align_sentences:
        logger.info("Updating time-alignment for merged files")
        aligned_dir = config.dir('aligned', create=True)
        for merged_file in config.dir('merged').glob('*-merged.json'):
            session = merged_file.name[:5]
            if args.limit_to_period and not session.startswith(str(args.period)):
                continue
            if args.limit_session and not re.match(args.limit_session, session):
                continue
            aligned_file = config.file(session, 'aligned')
            if (not aligned_file.exists() or
                aligned_file.stat().st_mtime < merged_file.stat().st_mtime):
                align_audiofile(merged_file, aligned_file, args.lang, args.cache_dir)
                publish_as_processed([ ( session, aligned_file ) ])

    # NER aligned files
    if args.extract_entities:
        logger.info("Updating NER for aligned files")
        ner_dir = config.dir('ner', create=True)
        for aligned_file in config.dir('aligned').glob('*-aligned.json'):
            session = aligned_file.name[:5]
            if args.limit_to_period and not session.startswith(str(args.period)):
                continue
            if args.limit_session and not re.match(args.limit_session, session):
                continue
            ner_file = config.file(session, 'ner')
            if (not ner_file.exists() or
                ner_file.stat().st_mtime < aligned_file.stat().st_mtime):
                extract_entities_from_file(aligned_file, ner_file, args)
                publish_as_processed([ ( session, ner_file ) ])

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
    parser.add_argument("--lang", type=str, default="deu",
                        help="Language")

    parser.add_argument("--download-original", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Download original files")
    parser.add_argument("--limit-to-period", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Limit time align and NER to specified period files")
    parser.add_argument("--limit-session", action="store",
                        default="",
                        help="Limit time align and NER to sessions matching regexp (eg 2001. for all 2001* sessions)")
    parser.add_argument("--align-sentences", action="store_true",
                        default=False,
                        help="Do the sentence alignment for downloaded sentences")
    parser.add_argument("--extract-entities", action="store_true",
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

    if args.cache_dir is None:
        args.cache_dir = args.data_dir / "cache"
    else:
        args.cache_dir = Path(args.cache_dir)

    execute_workflow(args)
