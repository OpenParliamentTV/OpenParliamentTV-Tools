#! /usr/bin/env python3

# Update a media directory

import logging
logger = logging.getLogger(__name__)

import argparse
import json
from pathlib import Path
from random import random
import re
import sys
import time

# Allow relative imports if invoked as a script
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

from .fetch_media import download_meeting_data, download_data, get_filename, parse_media_data, save_if_changed

# Max time to wait between retries (in seconds)
RETRY_MAX_WAIT_TIME = 10

def update_media_from_raw(media_dir):
    """Update media files that are older than raw media data, or non-existent
    """
    media_dir = Path(media_dir)
    for raw in sorted(media_dir.glob("raw-*.json")):
        parsed = media_dir / raw.name[4:]
        if (not parsed.exists() or
            raw.stat().st_mtime > parsed.stat().st_mtime):
            # Need an update
            with open(raw) as f:
                raw_data = json.load(f)
            data = parse_media_data(raw_data)
            save_if_changed(data, parsed)

def update_media_directory_period(period, media_dir, force=False, save_raw_data=False, retry_count=0):
    """Update the media directory by fetching items related to period.
    """
    # Fetch root page for period. This will allow us to determine the
    # most recent meeting number and then try to fetch them when needed
    rootinfo = download_meeting_data(period, media_dir, root_only=True)
    if not rootinfo['entries']:
        logger.error(f"No entries for period {period} - maybe a server timeout?")
        return
    # Get latest Sitzung/meeting number from first entry title
    latest_title = rootinfo['entries'][0]['title']
    numbers = re.findall(r'\((\d+)\.\sSitzung', latest_title)
    if not numbers:
        logger.error(f"Cannot determine latest meeting number from latest entry: {latest_title}")
        return
    latest_number = int(numbers[0])
    logger.info(f"Download period {period} meetings from {latest_number} downwards" )
    for meeting in range(latest_number, 0, -1):
        filename = get_filename(period, meeting)
        # We ignore cache if the force option is given, but also for
        # the latest meeting, since we may be updating a live meeting
        # which is updated throughout the session.  We assume here
        # that once a new session has begun, the previous ones are
        # "solid" so we can use cached information.
        should_retry = retry_count
        if (force
            or meeting == latest_number
            or not (media_dir / filename).exists()):
            logger.debug(f"Loading {period}-{meeting} data into {filename}")
            while should_retry >= 0:
                raw_data, data = download_data(period,
                                               meeting,
                                               media_dir,
                                               save_raw_data=save_raw_data,
                                               force=(force or meeting == latest_number))
                if data:
                    # Download success. Stop trying.
                    should_retry = -1
                else:
                    should_retry -= 1
                    if should_retry >= 0:
                        timeout = random() * RETRY_MAX_WAIT_TIME
                        logger.warning(f"Loading error - retrying in {timeout:.2f} seconds")
                        time.sleep(timeout)
                    else:
                        logger.warning("Too many failed retries. Cancelling update_media")
                        return
            # Sessions numbered 9XX seem to be special sessions, we do not want to
            # try to download previous ones.
            if re.match(str(meeting), r'^9\d\d$'):
                logger.info(f"Special session {meeting} - stopping descending download" )
                return


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Update media files.")
    parser.add_argument("media_dir", type=str, nargs='?',
                        help="Media directory (output)")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    parser.add_argument("--retry-count", type=int,
                        dest="retry_count", default=0,
                        help="Max number of times to retry a media download")
    parser.add_argument("--from-period", type=int,
                        help="Period to fetch")
    parser.add_argument("--force", dest="force", action="store_true",
                        default=False,
                        help="Force loading of data for a meeting even if the corresponding file already exists")
    parser.add_argument("--save-raw-data", dest="save_raw_data", action="store_true",
                        default=False,
                        help="Save raw data in JSON format in addition to converted JSON data. It will be an object with 'root' (first page) and 'entries' (all entries for the period/meeting) keys.")

    args = parser.parse_args()

    if args.media_dir is None:
        parser.print_help()
        sys.exit(1)

    loglevel = logging.INFO
    if args.debug:
        loglevel=logging.DEBUG
    logging.basicConfig(level=loglevel)

    if args.from_period is not None:
        update_media_directory_period(args.from_period, Path(args.media_dir), force=args.force, save_raw_data=args.save_raw_data, retry_count=args.retry_count)
    else:
        # Update the media directory using cached raw- files
        update_media_from_raw(Path(args.media_dir))
