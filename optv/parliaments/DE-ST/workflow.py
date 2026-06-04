#! /usr/bin/env python3
"""DE-ST Landtag Sachsen-Anhalt workflow.

Scrapes the portal's per-Sitzungsperiode HTML pages and their associated
per-speech video AJAX endpoints, splits each page into per-Landtagssitzung
intermediate JSONs, then merges, links, aligns, and NERs them through the
shared pipeline. Stage orchestration lives in ``optv.shared.workflow``.
"""

import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-ST.workflow`.
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
from .scraper.fetch_archive import fetch_archive_and_build_sitzung_map
from .scraper.fetch_media import fetch_media_for_session_map
from .scraper.fetch_sessions import fetch_session_pages

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    metadata_dir = config.dir('nel_data', create=True)
    sitzung_map = fetch_archive_and_build_sitzung_map(
        period=args.period,
        proceedings_dir=config.dir('proceedings', create=True),
        metadata_dir=metadata_dir,
        force=args.force,
        retry_count=args.retry_count,
    )
    fetch_session_pages(
        sitzung_map=sitzung_map,
        proceedings_dir=config.dir('proceedings'),
        force=args.force,
        retry_count=args.retry_count,
    )
    fetch_media_for_session_map(
        sitzung_map=sitzung_map,
        proceedings_dir=config.dir('proceedings'),
        media_dir=config.dir('media', create=True),
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )


def _parse(config, args):
    metadata_dir = config.dir('nel_data')
    parse_proceedings_directory(
        config.dir('proceedings'),
        media_dir=config.dir('media'),
        metadata_dir=metadata_dir,
    )
    parse_media_directory(config.dir('media'))


def _merge(config, session, args):
    return merge_session(session, config, args)


def _align(config, session, args):
    merged_file = config.file(session, 'merged')
    aligned_file = config.file(session, 'aligned', create=True)
    align_audiofile(merged_file, aligned_file, args.aeneas_language, args.cache_dir,
                    timeout=args.align_timeout,
                    max_audio_seconds=args.align_max_audio_seconds)
    return aligned_file


def _session_in_scope(args, session: str) -> bool:
    """DE-ST session keys are 5-digit ``{period:02d}{sitzung:03d}`` (e.g. 08105).

    The shared default uses ``session.startswith(str(args.period))`` which
    fails for single-digit periods because the zero-padding hides the prefix.
    Match on the zero-padded period prefix instead.
    """
    import re
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
    align_session_to_file=_align,
    session_in_scope=_session_in_scope,
)


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-ST Landtag Sachsen-Anhalt workflow.",
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
