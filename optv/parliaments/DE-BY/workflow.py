#! /usr/bin/env python3
"""DE-BY Bayerischer Landtag workflow.

Drives the "Plenum Online" PrimeFaces accordion (``sitzungsablauf_accordion``)
to build a session index, fetches the per-TOP ``meta_vod`` playlists, then
parses, merges, and NEL-links the per-Sitzung speeches through the shared
pipeline. Stage orchestration lives in ``optv.shared.workflow``.

Text comes from the joined Plenarprotokoll spine (``join_text_to_spine`` in the
merger; the source text is § 5 Abs. 2 UrhG, free to reuse), so ``align`` and
``ner`` are wired: ``_align`` stages per-speech audio (each BY speech has its own
HLS clip — no slicing) and runs aeneas; NER is parliament-agnostic and driven by
the shared runner.
"""

import datetime as _dt
import json
import logging
import os
import re
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-BY.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.align import align_audio
from optv.shared.workflow import WorkflowHooks, run_main

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


def _merge(config, session, args):
    return merge_session(session, config, args)


def _align(config, session, args):
    merged_file = config.file(session, "merged")
    if not merged_file.exists():
        raise FileNotFoundError(f"[{session}] no merged file — cannot align")
    doc = json.loads(merged_file.read_text())

    logger.info(f"[{session}] staging per-speech audio")
    prepare_per_speech_audio(doc["data"], args.cache_dir, force=args.force)

    logger.info(f"[{session}] running aeneas alignment ({args.aeneas_language})")
    align_audio(doc["data"], language=args.aeneas_language, cachedir=args.cache_dir,
                force=args.force, timeout=args.align_timeout,
                max_audio_seconds=args.align_max_audio_seconds)
    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    doc["meta"].setdefault("processing", {})["align"] = now
    doc["meta"]["lastProcessing"] = "align"
    doc["meta"]["lastUpdate"] = now

    aligned_file = config.file(session, "aligned", create=True)
    aligned_file.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {aligned_file.name}")
    return aligned_file


def _session_in_scope(args, session: str) -> bool:
    """DE-BY session keys are 5-digit ``{period:02d}{sitzung:03d}`` (e.g. 19054).

    Mirror DE-SH: the shared default ``session.startswith(str(args.period))``
    is correct for two-digit periods (19…), but the explicit override keeps a
    future single-digit term safe via zero-padding.
    """
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
        description="DE-BY Bayerischer Landtag workflow.",
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
