#! /usr/bin/env python3
"""DE-BY Bayerischer Landtag workflow.

Drives the "Plenum Online" PrimeFaces accordion (``sitzungsablauf_accordion``)
to build a session index, fetches the per-TOP playlists, then
parses, merges, and NEL-links the per-Sitzung speeches through the shared
pipeline. Stage orchestration lives in ``optv.shared.workflow``.

Text comes from the joined Plenarprotokoll spine (``join_text_to_spine`` in the
merger), so ``align`` and ``ner`` are wired: the align hook stages per-speech
audio (each BY speech has its own HLS clip — no slicing) and runs aeneas; NER is
parliament-agnostic and driven by the shared runner. This text+align path is
**experimental and unvalidated** — no Whisper-QC / text-fidelity audit has
cleared it yet, and substantial refinement is still needed before platform
integration.
"""

import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-BY.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.audio_prep import make_align_hook
from optv.shared.workflow import WorkflowHooks, adapt_merge, period_prefix_scope, run_main

from .align_prep import prepare_per_speech_audio
from .common import Config
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_directory
from .parsers.proceedings2json import parse_proceedings_directory
from .scraper.fetch_archive import fetch_archive
from .scraper.fetch_media import fetch_media_for_archive
from .scraper.fetch_proceedings import fetch_proceedings

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    archive = fetch_archive(
        period=args.period,
        media_dir=config.dir('media', create=True),
        metadata_dir=config.dir('nel_data', create=True),
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
    fetch_media_for_archive(
        archive=archive,
        media_dir=config.dir('media'),
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
    fetch_proceedings(config, args)


def _parse(config, args):
    parse_media_directory(config.dir('media'))
    parse_proceedings_directory(config, args)


_merge = adapt_merge(merge_session)


# Session keys are 5-digit ``{period:02d}{sitzung:03d}`` (e.g. 19054); the
# zero-padded prefix also matches single-digit periods.
_session_in_scope = period_prefix_scope(width=2)


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=make_align_hook(prepare_per_speech_audio),
    session_in_scope=_session_in_scope,
)


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-BY Bayerischer Landtag workflow.",
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
