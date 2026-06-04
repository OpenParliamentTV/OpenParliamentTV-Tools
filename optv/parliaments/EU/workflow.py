#! /usr/bin/env python3
"""European Parliament (EU) workflow.

Downloads plenary verbatim proceedings from the EP Open Data Portal API
(``data.europarl.europa.eu/api/v2``) and glcloud SSR event JSON, parses both,
merges them into Stage 2 JSON, then optionally aligns, links entities, and
extracts entities. Stage orchestration is shared via ``optv.shared.workflow``.

Period semantics: ``--period=10`` is term 10 (2024–2029). Session keys are
``YYYYMMDD`` (one plenary day = one session). Because EU session keys don't
start with the period string, we override ``session_in_scope`` to validate
that the date falls inside term 10's date range (2024-07-16 onwards).

Date selection (priority order):
  1. ``--eu-date YYYY-MM-DD`` (repeatable) — explicit list of plenary days
  2. ``--year YYYY`` — enumerate all plenary sittings for the calendar year
  3. ``--limit-session YYYYMMDD`` — single-day shortcut

The download stage is a no-op if none of these are provided.
"""

import datetime as _dt
import json
import logging
import os
import sys
import re
from pathlib import Path

# Allow both ``./workflow.py`` and ``python -m optv.parliaments.EU.workflow``.
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
from .scraper.fetch_media import download_media
from .scraper.fetch_proceedings import download_proceedings

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name

# Term 10 began 2024-07-16. The lower bound is a hard sanity check; the upper
# bound is loose (we'll know when term 11 begins).
TERM_10_START = "20240716"


def _session_in_scope(args, session):
    """EU session keys are ``YYYYMMDD`` (plenary days). Filter by:
      * ``--limit-session`` regex (literal date or pattern), and
      * date >= term 10 start (when ``--limit-to-period`` is on)."""
    if not re.fullmatch(r"\d{8}", session):
        return False
    if args.limit_to_period and session < TERM_10_START:
        return False
    if args.limit_session:
        try:
            if not re.match(args.limit_session, session):
                return False
        except re.error:
            if args.limit_session != session:
                return False
    return True


def _download(config, args):
    download_proceedings(config, args)
    download_media(config, args)


def _parse(config, args):
    parse_proceedings_directory(config, args)
    parse_media_directory(config, args)


def _merge(config, session, args):
    """merge_session writes the file itself and returns its path."""
    return merge_session(session, config, args)


def _align(config, session, args):
    """Single-language audio alignment — all EU speech text comes from the API
    in English, so aeneas runs with ``eng`` (espeak voice) across the whole
    session."""
    merged_file = config.file(session, "merged")
    if not merged_file.exists():
        raise FileNotFoundError(f"[{session}] no merged file — cannot align")

    logger.info(f"[{session}] preparing per-speech audio slices")
    doc = json.loads(merged_file.read_text())
    prepare_per_speech_audio(doc["data"], args.cache_dir, force=args.force)

    aeneas_lang = getattr(args, "aeneas_language", None) or "eng"
    logger.info(f"[{session}] aeneas align ({len(doc['data'])} speeches, lang={aeneas_lang})")
    align_audio(
        doc["data"],
        language=aeneas_lang,
        cachedir=args.cache_dir,
        force=args.force,
        timeout=args.align_timeout,
        max_audio_seconds=args.align_max_audio_seconds,
    )

    doc["meta"].setdefault("processing", {})
    doc["meta"]["processing"]["align"] = _dt.datetime.utcnow().isoformat(timespec="seconds")
    doc["meta"]["lastProcessing"] = "align"
    doc["meta"]["lastUpdate"] = _dt.datetime.utcnow().isoformat(timespec="seconds")

    aligned_file = config.file(session, "aligned", create=True)
    aligned_file.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {aligned_file.name}")
    return aligned_file


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
    session_in_scope=_session_in_scope,
)


def _add_arguments(parser):
    """Parliament-specific flags beyond the shared set."""
    parser.add_argument("--session", dest="eu_date", action="append", default=[],
                        help="Plenary date YYYY-MM-DD or YYYYMMDD (repeatable). "
                             "Required for --download-original unless --year is set.")
    parser.add_argument("--year", type=int, default=None,
                        help="Auto-enumerate every plenary sitting for this calendar year "
                             "via the EP Open Data API (/meetings?year=YYYY).")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="EU European Parliament workflow: download → parse → merge → "
                    "align → NEL → NER.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
