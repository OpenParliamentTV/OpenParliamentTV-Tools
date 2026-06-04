#! /usr/bin/env python3
"""ES Congreso de los Diputados workflow.

Fetches per-speech interventions (video + metadata) and per-session HTML
Diario de Sesiones, parses each into the intermediate JSON shape, merges
them per session, then optionally aligns, links entities, and extracts
entities. Stage orchestration is shared via ``optv.shared.workflow``.
"""

import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.ES.workflow`.
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
from .scraper.fetch_interventions import update_interventions_period
from .scraper.fetch_proceedings import download_proceedings_period

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    logger.info(f"Downloading interventions and proceedings for period {args.period}")
    # Per-speech video + metadata feed (defines the set of sessions).
    update_interventions_period(args.period, config.dir('media'),
                                force=args.force, retry_count=args.retry_count)
    # Per-session HTML Diario de Sesiones (text source).
    download_proceedings_period(args.period, config.dir('proceedings'), config.dir('media'),
                                force=args.force, retry_count=args.retry_count)


def _parse(config, args):
    parse_media_directory(config.dir('media'))
    parse_proceedings_directory(config.dir('proceedings'), args)


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


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="Fetch, parse and merge Congreso de los Diputados interventions and proceedings.",
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
