#! /usr/bin/env python3
"""DE-RP Rheinland-Pfalz Landtag workflow.

Ingests ePP XML proceedings from a delivery inbox and OPAL search HTML from
a media inbox (no live scrapers yet — both are pushed manually), merges
them per session, then optionally aligns, links entities, and extracts
entities. Stage orchestration is shared via ``optv.shared.workflow``.
"""

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
from optv.shared.workflow import WorkflowHooks, run_main

from .common import Config
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_directory
from .parsers.proceedings2json import parse_proceedings_directory
from .scraper.fetch_media import ingest_html_inbox
from .scraper.ingest_xml import ingest_inbox

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    proceedings_dir = config.dir('proceedings', create=True)
    if args.inbox_dir:
        logger.info("Ingesting ePP XML proceedings from inbox")
        ingest_inbox(Path(args.inbox_dir), proceedings_dir)
    else:
        logger.info("No --inbox-dir given; skipping ePP ingest")

    logger.info("Ingesting OPAL search HTML from media inbox")
    media_dir = config.dir('media', create=True)
    media_inbox = Path(args.media_inbox_dir) if args.media_inbox_dir else (media_dir / "inbox")
    ingest_html_inbox(media_inbox, media_dir)


def _parse(config, args):
    parse_proceedings_directory(config.dir('proceedings'), args)
    parse_media_directory(config.dir('media'))


def _merge(config, session, args):
    return merge_session(session, config, args)


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


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-RP Landtag workflow.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
