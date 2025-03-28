#! /usr/bin/env python3

# Fetch Proceedings for Bundestag
# It must be given an output directory (like examples/proceedings) and will fetch only missing files.

# Adapted from
# https://blog.oliverflasch.de/german-plenary-proceedings-as-a-nlp-testbed/

import logging
logger = logging.getLogger(__name__)

import argparse
import lxml.html
import os
from pathlib import Path
import re
import sys
import urllib.request
import urllib3

SERVER_ROOT = "https://www.bundestag.de"

AJAX_ID = {
    # Ajax ID Period 19
    19: "543410-543410",
    # Ajax ID Period 20
    20: "866354-866354",
    # Ajax ID Period 21
    21: "1058442-1058442"
}

def download_plenary_protocols(destination_dir: str, fullscan: bool = False, period: int = 20) -> "list[(str, str)]":
    """Download and stores proceedings

    Returns a list of (filename, url) for downloaded files.
    """
    if not AJAX_ID.get(period):
        return []
    dest = Path(destination_dir)
    # Create directory if necessary
    if not dest.is_dir():
        dest.mkdir(parents=True)
    http = urllib3.PoolManager()
    created_files = []
    offset = 0
    while True:
        logger.debug(f"Fetching RSS with offset {offset}")
        response = http.request("GET", f"{SERVER_ROOT}/ajax/filterlist/de/services/opendata/{AJAX_ID[period]}?noFilterSet=true&offset={offset}")
        parsed = lxml.html.fromstring(response.data)
        link_count = 0
        for link in parsed.getiterator(tag="a"):
            link_href = link.attrib["href"]
            link_count += 1
            basename = os.path.basename(link_href)
            # Get session id from filename.
            # The basename is either NNNNN-data.xml or NNNNN.xml (from 20138 on)
            ids = re.findall(r'^(\d+)', basename)
            if not ids:
                raise ValueError(f"Invalid filename {basename} - cannot extract session id")
            session_id = ids[0]
            basename = f"{session_id}-proceedings.xml"
            filename = dest / basename
            if filename.exists():
                # Existing file.
                if not fullscan:
                    logger.info(f"Found cached file {filename}. Stopping.")
                    return created_files
            else:
                # Download file
                #file_url = f"{SERVER_ROOT}{link_href}"
                file_url = f"{link_href}"
                logger.info(f"downloading URL {file_url}")
                with urllib.request.urlopen(file_url) as f:
                    # Add source URL as a processing instruction, but
                    # after the XML declaration. The cleanest way
                    # would be to parse the whole XML and use some
                    # API, but a simple text-based approach is more economical.
                    pi = f"""<?source url="{file_url}"?>\n""".encode('utf-8')
                    with open(filename, 'wb') as out:
                        first_line = f.readline()
                        # We make sure to preserve the XML declaration
                        # at the start. If there is no XML
                        # declaration, then we put the PI first.
                        # Note: some files have the BOM b'\xef\xbb\xbf'
                        # at the beginning, so we cannot test with .startswith
                        if b'<?xml' in first_line:
                            out.write(first_line)
                            out.write(pi)
                        else:
                            out.write(pi)
                            out.write(first_line)
                        # Write the rest of the file
                        out.write(f.read())
                created_files.append( (filename, file_url) )
        if link_count == 0:
            # Empty file, end of data
            break
        offset += link_count
    return created_files

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Fetch Bundestag RSS feed.")
    parser.add_argument("output_dir", metavar="output_dir", type=str, nargs='?',
                        help="Output directory")
    parser.add_argument("--period", type=int,
                        default=20,
                        help="Period to fetch (default: 20)")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    parser.add_argument("--full-scan", dest="fullscan", action="store_true",
                        default=False,
                        help="Do a full scan of the RSS feed (else we stop at the first existing file)")
    args = parser.parse_args()
    if args.output_dir is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel=logging.DEBUG
    logging.basicConfig(level=loglevel)
    download_plenary_protocols(args.output_dir, args.fullscan, args.period)
