#! /usr/bin/env python3
"""DE-HH Hamburgische Bürgerschaft workflow.

Enumerates session video pages (predictable ``/sitzung/{WP}/{n}/`` URLs, bounded
by the mediathek's plenary list, + operator-supplied URLs), fetches each page's
static per-speech spine, the Plenarprotokoll PDFs and — for sittings without a
protocol yet — the per-speech WebVTT subtitles, then parses, merges and
NEL-links the per-Sitzung speeches through the shared pipeline. Stage
orchestration lives in ``optv.shared.workflow``.

Each speech's video is its own server-cut HLS clip. Text is the protocol once
it is published (parsed via ``optv.shared.pdf2tei`` and joined onto the media
spine), else the subtitles. The align hook follows the text source: subtitle
text is already timed against the speech clip, so it is only stamped
(``debug.alignSource = "subtitles"``, no aeneas); protocol text is aligned by
aeneas against the speech clip. The protocol text+align path is **experimental
and unvalidated** — no Whisper-QC / text-fidelity audit has cleared it yet, and
substantial refinement is still needed before platform integration.

A protocol that arrives for a sitting already published with subtitle text is
queued in ``cache/de-hh-protocol-upgrades.txt``; the ``update`` wrapper re-runs
those sittings with ``--rebuild`` (the publish guard refuses a changed transcript
otherwise).
"""

import datetime as _dt
import json
import logging
import os
import re
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-HH.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.audio_prep import make_align_hook
from optv.shared.publish import richest_source
from optv.shared.workflow import WorkflowHooks, run_main

from .align_prep import prepare_per_speech_audio
from .common import Config
from .merger.merge_session import merge_session
from .parsers.proceedings2json import parse_proceedings_directory
from .scraper.fetch_proceedings import fetch_proceedings
from .parsers.media2json import parse_media_directory
from .scraper.fetch_archive import fetch_archive
from .scraper.fetch_media import fetch_media_for_archive
from .scraper.fetch_subtitles import fetch_subtitles

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    archive = fetch_archive(
        period=args.period,
        media_dir=config.dir('media', create=True),
        metadata_dir=config.dir('nel_data', create=True),
        seed_urls=getattr(args, "session_url", None),
        max_session=getattr(args, "max_session", None),
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
    fetch_subtitles(
        media_dir=config.dir('media'),
        proceedings_dir=config.dir('proceedings'),
        subtitles_dir=config.dir('subtitles', create=True),
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )


def _parse(config, args):
    parse_media_directory(config.dir('media'), config.dir('subtitles'))
    parse_proceedings_directory(config, args)


def _merge(config, session, args):
    return merge_session(session, config, args)


def _session_in_scope(args, session: str) -> bool:
    """DE-HH session keys are 5-digit ``{period:02d}{sitzung:03d}`` (e.g. 23018).

    Mirror DE-BW/DE-BY/DE-SH: the shared default
    ``session.startswith(str(args.period))`` is correct for two-digit periods
    (22/23…), but the explicit override keeps a future single-digit term safe
    via zero-padding.
    """
    if args.limit_to_period and not session.startswith(f"{int(args.period):02d}"):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


_aeneas_align = make_align_hook(prepare_per_speech_audio)


def stamp_subtitle_timing(data: list[dict]) -> int:
    """Mark subtitle-text speeches as aligned: their sentences are already timed
    against the speech clip. Returns the number of speeches stamped."""
    stamped = 0
    for speech in data:
        dbg = speech.get("debug") or {}
        if dbg.get("textSource") != "subtitles":
            continue
        ends = [float(s["timeEnd"])
                for tc in speech.get("textContents") or [] if tc.get("type") == "generated"
                for body in tc.get("textBody") or []
                for s in body.get("sentences") or [] if s.get("timeEnd") is not None]
        if not ends:
            continue
        speech["debug"]["alignDuration"] = round(max(ends), 3) or 0.001
        speech["debug"]["alignSource"] = "subtitles"
        speech.setdefault("media", {})["aligned"] = True
        stamped += 1
    return stamped


def _align(config, session, args):
    """Stamp subtitle timing, or run aeneas over protocol text."""
    source_file = richest_source(config, session)
    if not source_file.exists():
        raise FileNotFoundError(f"[{session}] no merged file — cannot align")
    doc = json.loads(source_file.read_text())
    if not any((s.get("debug") or {}).get("textSource") == "subtitles"
               for s in doc.get("data") or []):
        return _aeneas_align(config, session, args)

    stamped = stamp_subtitle_timing(doc["data"])
    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    doc["meta"].setdefault("processing", {})["align"] = now
    doc["meta"]["lastProcessing"] = "align"
    doc["meta"]["lastUpdate"] = now
    aligned_file = config.file(session, "aligned", create=True)
    aligned_file.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] subtitle timing for {stamped}/{len(doc['data'])} speeches "
                f"— wrote {aligned_file.name}")
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
    parser.add_argument("--session", dest="session_url", action="append", default=[],
                        help="Session video-page URL to include in the archive "
                             "(repeatable). Manual override for the discovery "
                             "enumeration.")
    parser.add_argument("--max-session", type=int, default=None,
                        help="Upper Sitzung bound for the discovery enumeration "
                             "(default: read from the mediathek's plenary list).")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-HH Hamburgische Bürgerschaft workflow.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
