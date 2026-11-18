#! /usr/bin/env python3
"""DE-RP Rheinland-Pfalz Landtag workflow.

Ingests ePP XML proceedings from a delivery inbox and OPAL search HTML from
a media inbox (no live scrapers — both are pushed manually; ``--fetch-opal``
saves an OPAL search with a headless browser for test runs), merges them per
session, then optionally aligns, links entities, and extracts entities.
Stage orchestration is shared via ``optv.shared.workflow``.
"""

from datetime import date
import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-RP.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.align import align_audiofile
from optv.shared.workflow import WorkflowHooks, adapt_merge, run_main

from .common import Config
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_directory
from .parsers.proceedings2json import parse_proceedings_directory
from .scraper.fetch_media import ingest_html_inbox
from .scraper.fetch_opal import fetch_speech_search
from .scraper.ingest_xml import ingest_inbox

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    proceedings_dir = config.dir('proceedings', create=True)
    inbox = Path(args.inbox_dir) if args.inbox_dir else config.dir('data') / "inbox"
    logger.info("Ingesting ePP XML proceedings from inbox")
    ingest_inbox(inbox, proceedings_dir)

    media_dir = config.dir('media', create=True)
    media_inbox = Path(args.media_inbox_dir) if args.media_inbox_dir else (media_dir / "inbox")
    if args.fetch_opal:
        date_from, date_to = args.fetch_opal
        logger.info(f"Fetching OPAL speech search WP {args.period}, {date_from} to {date_to}")
        fetch_speech_search(args.period, date_from, date_to, media_inbox)

    logger.info("Ingesting OPAL search HTML from media inbox")
    ingest_html_inbox(media_inbox, media_dir,
                      fingerprint=args.fingerprint_media,
                      recheck=args.recheck_media)


def _parse(config, args):
    parse_proceedings_directory(config.dir('proceedings'), args)
    parse_media_directory(config.dir('media'))


_merge = adapt_merge(merge_session)


def _align(config, session, args):
    merged_file = config.file(session, 'merged')
    aligned_file = config.file(session, 'aligned', create=True)
    align_audiofile(merged_file, aligned_file, args.lang, args.cache_dir,
                    timeout=args.align_timeout,
                    max_audio_seconds=args.align_max_audio_seconds)
    return aligned_file


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
)


def _add_arguments(parser):
    """Parliament-specific flags beyond the shared set."""
    parser.add_argument("--inbox-dir", type=str, default=None,
                        help="ePP XML delivery inbox (default <data>/inbox)")
    parser.add_argument("--media-inbox-dir", type=str, default=None,
                        help="OPAL HTML inbox (default <data>/original/media/inbox)")
    parser.add_argument("--fingerprint-media", action="store_true", default=False,
                        help="HEAD each new OPAL video file (1 request/s) and record "
                             "Last-Modified and Content-Length")
    parser.add_argument("--recheck-media", action="store_true", default=False,
                        help="HEAD every OPAL video file again and flag files the "
                             "server replaced")
    parser.add_argument("--fetch-opal", nargs=2, type=date.fromisoformat, default=None,
                        metavar=("FROM", "TO"),
                        help="Before ingest, save OPAL's speech search for --period and "
                             "this date range (YYYY-MM-DD) into the media inbox via a "
                             "headless browser (test runs; needs requirements-browser.txt)")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-RP Landtag workflow.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
