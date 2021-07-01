#! /usr/bin/env python3

# Fetch Media items for Bundestag

# It will fetch and aggregate paginated data, either for a whole period or for a specific period + meeting

# It outputs data to stdout, or
# it can be given an output directory (like examples/media) through the --output option

# For reference, base URLs are like
# http://webtv.bundestag.de/player/macros/bttv/podcast/video/plenar.xml?period=17&meetingNumber=190

import logging
logger = logging.getLogger(__name__)

import argparse
import feedparser
import json
from pathlib import Path
import sys

try:
    from parsers.media2json import parse_media_data
except ModuleNotFoundError:
    # Module not found. Tweak the sys.path
    base_dir = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(base_dir))
    from parsers.media2json import parse_media_data

ROOT_URL = "http://webtv.bundestag.de/player/macros/bttv/podcast/video/plenar.xml"
SERVER_ROOT = "https://www.bundestag.de"

def get_latest():
    latest = feedparser.parse(ROOT_URL)
    return latest

def next_rss(data):
    feed = data.get('feed')
    if feed is None:
        return None
    links = feed.get('links')
    if not links:
        return None
    nexts = [ l for l in links if l.get('rel') == 'next' ]
    if nexts:
        return nexts[0]['href']
    else:
        return None

def download_period_data(period: str):
    root_url = f"{ROOT_URL}?period={period}"
    logging.warning(f"Downloading {root_url}")
    root = feedparser.parse(root_url)
    entries = root['entries']
    next_url = next_rss(root)
    while next_url:
        logging.warning(f"Downloading {next_url}")
        data = feedparser.parse(next_url)
        entries.extend(data['entries'])
        next_url = next_rss(data)
    return { "root": root,
             "entries": entries }

def download_meeting_data(period: int, number: int):
    """Download data for a given meeting, handling pagination.
    """
    # feedparser.parse(f"{ROOT_URL}?period={period}&meetingNumber={number}")
    root = feedparser.parse(f"{ROOT_URL}?period={period}&meetingNumber={number}")
    if root['status'] == 503:
        # Frequent error from server. We should retry. For the moment,
        # this will be done by re-running the script, since it will
        # only update necessary files.
        return { 'root': root, 'entries': [] }
    entries = root['entries']
    next_url = next_rss(root)
    while next_url:
        logging.warning(f"Downloading {next_url}")
        data = feedparser.parse(next_url)
        entries.extend(data['entries'])
        next_url = next_rss(data)
    return { "root": root,
             "entries": entries }

def get_filename(period, meeting=None):
    if meeting is None:
        # Only period is specified
        return f"{period}-all-media.json"
    else:
        return f"{period}{meeting.rjust(3, '0')}-media.json"

def download_data(period, meeting=None, output=None):
    filename = get_filename(period, meeting)
    try:
        if meeting is None:
            # Only period is specified
            data = download_period_data(period)
        else:
            data = download_meeting_data(period, meeting)

        if not data['entries']:
            # No entries - something must have gone wrong. Bail out
            logger.warning(f"No data ({data['root']['status']})")
            # import IPython; IPython.embed()
            return
        data = parse_media_data(data)

    except:
        logger.exception("Error")
        import IPython; IPython.embed()

    if output:
        output_dir = Path(output)
        if not output_dir.is_dir():
            output_dir.mkdir(parents=True)
        with open(output_dir / filename, 'w') as f:
            json.dump(data, f, indent=2)
    else:
        # No output dir option - dump to stdout
        json.dump(data, sys.stdout, indent=2)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Fetch Bundestag Media RSS feed.")
    parser.add_argument("period", metavar="period", type=str, nargs='?',
                        help="Period number (19 is the latest)")
    parser.add_argument("meeting", metavar="meeting", type=str, nargs='?',
                        help="Meeting number")
    parser.add_argument("--output", type=str, default="",
                        help="Output directory")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    parser.add_argument("--full-scan", dest="fullscan", action="store_true",
                        default=False,
                        help="Do a full scan of the RSS feed (else we stop at the first existing file)")
    args = parser.parse_args()
    if args.period is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel=logging.DEBUG
    logging.basicConfig(level=loglevel)
    download_data(args.period, args.meeting, args.output)