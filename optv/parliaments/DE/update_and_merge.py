#! /usr/bin/env python3

# Update media files, proceeding files and merge them
import argparse
import logging
import os
from pathlib import Path
import shutil
import sys

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

from aligner.align_sentences import align_audiofile
from ner.ner import extract_entities_from_file
from scraper.update_media import update_media_directory_period
from scraper.fetch_proceedings import download_plenary_protocols
from merger.merge_session import merge_files_or_dirs
from parsers.proceedings2json import parse_proceedings_directory

def update_and_merge(args):
    # Download/parse new media data
    update_media_directory_period(args.from_period,
                                  args.media_dir,
                                  force=args.force,
                                  save_raw_data=args.save_raw_data,
                                  retry_count=args.retry_count)

    # Download new proceedings data
    download_plenary_protocols(args.proceedings_dir, False, args.from_period)

    # Update all proceedings that need to be updated
    parse_proceedings_directory(args.proceedings_dir, args)

    # Produce merged data
    logger.info(f"Merging data from {args.media_dir} and {args.proceedings_dir} into {args.merged_dir}")

    # Produce merged data into args.merged_dir
    merged_files = merge_files_or_dirs(args.media_dir, args.proceedings_dir, args.merged_dir, args)

    # We keep track of the last produced file for each concerned
    # session. The keys can be updated in case of alignment/extraction.
    # It is used in the final step of copying final data into processed
    produced_files = dict(merged_files)

    # Time-align produced files
    if args.align_sentences:
        if not args.aligned_dir.is_dir():
            args.aligned_dir.mkdir(parents=True)
        for session, sourcefile in merged_files:
            filename = f"{session}-aligned.json"
            aligned_file = args.aligned_dir / filename
            # Since we use the output of merge_files_or_dirs we know
            # that merged_files will be newer anyway, even if a
            # previous alignmed file existed, so do not bother
            # checking for timestamps
            align_audiofile(sourcefile, aligned_file, args.lang, args.cache_dir)
            produced_files[session] = aligned_file

            if args.extract_entities:
                ner_file = args.ner_dir / f"{session}-ner.json"
                try:
                    extract_entities_from_file(aligned_file, ner_file, args)
                    produced_files[session] = ner_file
                except FileNotFoundError:
                    logger.error("Cannot extract entities from {ner_file} - File Not Found")

    # Finalizing step. Copy all produced_files into processed
    for session in sorted(produced_files.keys()):
        processed_file = args.processed_dir / f"{session}-session.json"
        shutil.copyfile(produced_files[session], processed_file)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Update media files corresponding to proceeding XML files.")
    parser.add_argument("data_dir", type=str, nargs='?',
                        help="Data directory - mandatory")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    parser.add_argument("--from-period", type=int,
                        help="Period to fetch (mandatory)")
    parser.add_argument("--include-nas", action="store_true",
                        help="Include T_NaS and T_fett classes as speech information for proceedings")
    parser.add_argument("--retry-count", type=int,
                        dest="retry_count", default=0,
                        help="Max number of times to retry a media download")
    parser.add_argument("--force", dest="force", action="store_true",
                        default=False,
                        help="Force loading of data for a meeting even if the corresponding file already exists")
    parser.add_argument("--save-raw-data", dest="save_raw_data", action="store_true",
                        default=False,
                        help="Save raw data in JSON format in addition to converted JSON data. It will be an object with 'root' (first page) and 'entries' (all entries for the period/meeting) keys.")
    parser.add_argument("--check", action="store_true",
                        default=False,
                        help="Check mergeability of files")
    parser.add_argument("--unmatched-count", action="store_true",
                        default=False,
                        help="Only display the number of unmatched proceeding items")
    parser.add_argument("--include-all-proceedings", action="store_true",
                        default=False,
                        help="Include all proceedings-issued speeches even if they did not have a match")
    parser.add_argument("--second-stage-matching", action="store_true",
                        default=False,
                        help="Do a second-stage matching using speaker names for non-matching subsequences")
    parser.add_argument("--advanced-rematch", action="store_true",
                        default=False,
                        help="Try harder to realign non-matching proceeding items by skipping some of the items")
    parser.add_argument("--complete", action="store_true",
                        default=False,
                        help="Add all necessary options for a full update (save raw data, include all proceedings)")
    parser.add_argument("--cache-dir", type=str, default=None,
                        help="Cache directory")
    parser.add_argument("--align-sentences", action="store_true",
                        default=False,
                        help="Do the sentence alignment for downloaded sentences")
    parser.add_argument("--extract-entities", action="store_true",
                        default=False,
                        help="Do Entity extraction on aligned sessions (requires --align-sentences)")
    parser.add_argument("--lang", type=str, default="deu",
                        help="Language")

    args = parser.parse_args()
    if args.data_dir is None or args.from_period is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel,
                        format='%(asctime)s %(levelname)-8s %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    if args.complete:
        # Force all options
        args.save_raw_data = True
        args.include_all_proceedings = True
        args.second_stage_matching = True
        args.advanced_rematch = True
        args.align_sentences = True
        args.extract_entities = True
        args.include_nas = True

    args.data_dir = Path(args.data_dir)

    if args.cache_dir is None:
        args.cache_dir = args.data_dir / "cache"
    else:
        args.cache_dir = Path(args.cache_dir)

    args.media_dir = args.data_dir / "original" / "media"
    args.proceedings_dir = args.data_dir / "original" / "proceedings"
    args.merged_dir = args.cache_dir / "merged"
    args.aligned_dir = args.cache_dir / "aligned"
    args.ner_dir = args.cache_dir / "ner"
    args.processed_dir = args.data_dir / "processed"

    update_and_merge(args)
