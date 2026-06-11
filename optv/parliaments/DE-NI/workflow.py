#! /usr/bin/env python3
"""DE-NI Niedersächsischer Landtag workflow.

Discovers Sitzungen via the Plenar-TV REST API (``api.plenartv.de``), fetches each
Sitzung's per-subject agenda + per-speech speaker timings, then parses, merges and
NEL-links the per-Sitzung speeches through the shared pipeline. Stage
orchestration lives in ``optv.shared.workflow``.

``align`` and ``ner`` are intentionally not registered for v1: the API already
provides the per-speech spine and the published Stage 2 carries video + speaker +
agenda metadata with an empty ``textContents`` list. Unlike DE-HH/DE-SH/DE-BW the
text is **not** PDF-locked — time-aligned WebVTT subtitles are available per
subject (``GET /vtt/{subject_id}``) and could re-enable text + sentence timings in
a later pass.
"""

import logging
import os
import re
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-NI.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.workflow import WorkflowHooks, run_main

from .common import Config
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_directory
from .parsers.vtt2json import parse_proceedings_directory
from .scraper.fetch_archive import fetch_archive
from .scraper.fetch_media import fetch_media_for_archive

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    archive = fetch_archive(
        period=args.period,
        metadata_dir=config.dir('nel_data', create=True),
        max_tagungsabschnitt=getattr(args, "max_tagungsabschnitt", None),
        force=args.force,
        retry_count=args.retry_count,
    )
    fetch_media_for_archive(
        archive=archive,
        media_dir=config.dir('media', create=True),
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )


def _parse(config, args):
    parse_media_directory(config.dir('media'))
    parse_proceedings_directory(config, args)


def _merge(config, session, args):
    return merge_session(session, config, args)


def _session_in_scope(args, session: str) -> bool:
    """DE-NI session keys are 5-digit ``{period:02d}{sitzung:03d}`` (e.g. 19080)."""
    if args.limit_to_period and not session.startswith(f"{int(args.period):02d}"):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=None,   # transcript text not wired in v1 — see manifest
    session_in_scope=_session_in_scope,
)


def _add_arguments(parser):
    """Parliament-specific flags beyond the shared set."""
    parser.add_argument("--max-tagungsabschnitt", type=int, default=None,
                        help="Upper Tagungsabschnitt bound for session discovery "
                             "(default: walk until consecutive misses).")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-NI Niedersächsischer Landtag workflow.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
