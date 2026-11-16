#! /usr/bin/env python3

# Fetch Proceedings for Bundestag
# It must be given an output directory (like examples/proceedings) and will fetch only missing files.

# Adapted from
# https://blog.oliverflasch.de/german-plenary-proceedings-as-a-nlp-testbed/

import logging
logger = logging.getLogger(__name__)

import argparse
import lxml.etree
import lxml.html
import os
from pathlib import Path
import re
import sys
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


class _ServerRefused(Exception):
    """bundestag.de answered something other than 200 — typically a 303 to its
    bot challenge (``/.enodia/challenge``), which a non-browser client cannot pass."""


def _get(http: urllib3.PoolManager, url: str) -> bytes:
    """GET ``url`` without following redirects; raise _ServerRefused unless 200.

    Following redirects would turn a bot challenge into a 200 HTML page that
    reads as an empty list, or gets stored as a protocol.
    """
    response = http.request("GET", url, redirect=False)
    if response.status != 200:
        location = response.headers.get("Location")
        raise _ServerRefused(f"HTTP {response.status} for {url}"
                             + (f" -> {location}" if location else ""))
    return response.data


def _protocol_links(page: bytes) -> "list[str]":
    """Protocol hrefs of one list page, in order, without duplicates.

    The page renders every entry twice (list view + table template), while the
    endpoint's ``offset`` counts entries — so the caller must advance by the
    de-duplicated count.
    """
    if not page.strip():
        return []
    parsed = lxml.html.fromstring(page)
    return list(dict.fromkeys(a.attrib["href"] for a in parsed.iter("a")
                              if "href" in a.attrib))


def _with_source_pi(data: bytes, url: str) -> bytes:
    """Insert ``<?source url="…"?>`` after the XML declaration (or first, if none).

    A simple text-based approach is more economical than parsing the whole
    XML. Some files start with the BOM b'\\xef\\xbb\\xbf', so the declaration
    is looked for in the first line rather than with startswith.
    """
    pi = f"""<?source url="{url}"?>\n""".encode('utf-8')
    newline = data.find(b"\n")
    first_line = data if newline < 0 else data[:newline + 1]
    if b'<?xml' in first_line:
        return first_line + pi + data[len(first_line):]
    return pi + data


def _is_protocol(xml_path: Path) -> bool:
    """True if ``xml_path`` is well-formed XML with a <dbtplenarprotokoll> root.

    Rejects what may arrive instead of a protocol — an HTML challenge or error
    page, a truncated body — none of which may be cached as a protocol, since
    cached files are never fetched again.
    """
    try:
        root = lxml.etree.parse(str(xml_path)).getroot()
    except Exception as e:
        logger.debug(f"{xml_path}: not well-formed XML ({e})")
        return False
    return str(root.tag).endswith('dbtplenarprotokoll')


def _verify_session_id(xml_path: Path, expected_id: str) -> bool:
    """True unless the XML's root wahlperiode/sitzung-nr definitely contradict
    expected_id (5-digit, period + 3-digit session). Returns True when the file
    isn't the native <dbtplenarprotokoll> format or the attributes are absent —
    we only HARD-reject a concrete mismatch, never an unverifiable file.

    Guards against the Bundestag occasionally serving the wrong protocol at the
    right URL (e.g. 21081.xml once contained sitzung-nr="80").
    """
    try:
        root = lxml.etree.parse(str(xml_path)).getroot()
    except Exception as e:
        logger.warning(f"{xml_path}: could not parse for session-id check ({e})")
        return True
    if not str(root.tag).endswith('dbtplenarprotokoll'):
        return True
    period = root.attrib.get('wahlperiode')
    session = root.attrib.get('sitzung-nr')
    if not period or not session or not session.isdigit():
        return True
    return period == expected_id[:-3] and int(session) == int(expected_id[-3:])


def _download(http: urllib3.PoolManager, file_url: str, target: Path, session_id: str) -> bool:
    """Fetch ``file_url`` into ``target`` via a ``.part`` file; True if stored.

    ``target`` only appears once the download is a well-formed protocol of
    ``session_id``. Anything else is discarded and the entry stays missing, so
    the next run retries it. Raises _ServerRefused on a non-200 answer.
    """
    data = _get(http, file_url)
    tmp = target.with_name(target.name + ".part")
    try:
        tmp.write_bytes(_with_source_pi(data, file_url))
        if not _is_protocol(tmp):
            logger.error(f"{file_url} is not a plenary protocol (challenge or error page?) "
                         f"- not storing {target.name}")
            return False
        if not _verify_session_id(tmp, session_id):
            logger.error(
                f"Session-id mismatch for {target}: filename expects {session_id} "
                f"but XML header disagrees — rejecting download from {file_url}"
            )
            return False
        os.replace(tmp, target)
        return True
    finally:
        tmp.unlink(missing_ok=True)


def download_plenary_protocols(destination_dir: str, fullscan: bool = False, period: int = 20) -> "list[(str, str)]":
    """Download missing proceedings; return a list of (filename, url) for new files.

    The opendata list is mostly but not strictly newest-first: an entry the
    Bundestag touches after a newer protocol came out is listed above it (e.g.
    21108 "Teilprotokoll" before 21109). So every entry of a page is checked,
    and the next page is only requested while a page holds nothing cached yet
    (cold cache, or more than a page of new protocols) — one list request per
    run in steady state. ``fullscan`` walks all pages.

    Cached files are never fetched again, even when the Bundestag later updates
    a protocol in place under the same URL; refreshing one means deleting it.

    A non-200 answer (bot challenge, outage) ends the run with a warning and
    the files fetched so far; it is never mistaken for the end of the list.
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
    try:
        while True:
            logger.debug(f"Fetching proceedings list with offset {offset}")
            hrefs = _protocol_links(_get(
                http, f"{SERVER_ROOT}/ajax/filterlist/de/services/opendata/{AJAX_ID[period]}"
                      f"?noFilterSet=true&offset={offset}"))
            if not hrefs:
                # Past the last page
                break
            page_had_cached = False
            for file_url in hrefs:
                basename = os.path.basename(file_url)
                # Get session id from filename.
                # The basename is either NNNNN-data.xml or NNNNN.xml (from 20138 on)
                ids = re.findall(r'^(\d+)', basename)
                if not ids:
                    raise ValueError(f"Invalid filename {basename} - cannot extract session id")
                session_id = ids[0]
                filename = dest / f"{session_id}-proceedings.xml"
                if filename.exists():
                    page_had_cached = True
                    continue
                logger.info(f"downloading URL {file_url}")
                if _download(http, file_url, filename, session_id):
                    created_files.append( (filename, file_url) )
            if page_had_cached and not fullscan:
                logger.info(f"Reached already cached proceedings at offset {offset}. Stopping.")
                break
            offset += len(hrefs)
    except _ServerRefused as e:
        logger.warning(f"bundestag.de refused the request ({e}) - bot challenge? "
                       f"Skipping the rest of this proceedings fetch")
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
                        help="Walk the whole list (else stop after the first page holding an already cached file)")
    args = parser.parse_args()
    if args.output_dir is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel=logging.DEBUG
    logging.basicConfig(level=loglevel)
    download_plenary_protocols(args.output_dir, args.fullscan, args.period)
