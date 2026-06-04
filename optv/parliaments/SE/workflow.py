#! /usr/bin/env python3
"""SE Riksdag workflow.

Downloads per-protokoll bundles from Riksdag (one MP3 per debate, per-speech
metadata), parses each into the intermediate JSON shape, merges them per
session, then optionally aligns, links entities, and extracts entities.
Stage orchestration is shared via ``optv.shared.workflow``.

Period semantics: ``--period`` is the riksmöte start year (e.g. ``2025`` for
riksmöte 2025/26). Session strings are ``{period}-{protokoll_nr:03d}``, so
``--limit-to-period`` filters by the ``2025-`` prefix (with dash; otherwise
``2025`` would match ``20251`` etc.).

Download stage requires ``--protokoll <dok_id>`` because Riksdag's
``anforandelista`` filter is unreliable (verified 2026-04-30): we cannot
auto-discover the protokoll list for a riksmöte from the API. Future
period-wide download support should layer on the bulk dataset URL pattern
``data.riksdagen.se/dataset/anforande/anforande-{rm_compact}.json.zip``.

NEL needs ``metadata/entities.json`` (Wikidata-derived Riksdag members +
parties); NER needs an Entity-Fishing endpoint with the ``db-sv`` KB loaded.
"""

import datetime as _dt
import json
import logging
import os
import re
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.SE.workflow`.
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
from .parsers.media2json import parse_media_for_session
from .parsers.proceedings2json import parse_bundle as parse_proceedings_bundle
from .scraper.fetch_session import fetch_session

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _session_in_scope(args, session):
    """SE uses ``2025-091`` session keys, so the period prefix needs the dash;
    otherwise ``--period=2025`` would match ``20251`` etc."""
    if args.limit_to_period and not session.startswith(str(args.period) + "-"):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


def _download(config, args):
    if not args.protokoll:
        sys.exit("--download-original requires --protokoll <dok_id> (e.g. --protokoll HD0991). "
                 "Riksdag's anforandelista filter is unreliable so we can't auto-discover protokollen.")
    for protokoll_id in args.protokoll:
        logger.info(f"Downloading protokoll {protokoll_id} into {config.dir('data')}")
        fetch_session(
            config, protokoll_id,
            force=args.force,
            retry_count=args.retry_count,
            retry_delay_max=args.retry_delay_max,
            limit_anforanden=args.limit_anforanden,
        )


def _parse(config, args):
    """Per-session parse: re-run only when the per-session bundle / per-debate
    media files are newer than the parsed output (Riksdag publishes one
    bundle per protokoll, so a directory-wide pass would do unnecessary work)."""
    sessions = [s for s in config.sessions() if _session_in_scope(args, s)]
    if not sessions:
        logger.info(f"No sessions in scope for period {args.period} — nothing to parse.")
        return

    media_dir = config.dir("media")
    debate_mtime = max(
        (p.stat().st_mtime for p in media_dir.glob("*-debatt.json")),
        default=0.0,
    )

    for session in sessions:
        bundle_path = config.dir("proceedings") / f"{session}-anforanden.json"
        proc_out = config.file(session, "proceedings")
        proc_stale = (
            args.force
            or not proc_out.exists()
            or bundle_path.stat().st_mtime > proc_out.stat().st_mtime
        )
        if proc_stale:
            logger.info(f"[{session}] parsing proceedings")
            bundle = json.loads(bundle_path.read_text())
            doc = parse_proceedings_bundle(bundle, args.spacy_model)
            proc_out.parent.mkdir(parents=True, exist_ok=True)
            proc_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {proc_out.name} ({len(doc['data'])} speeches)")

        media_out = config.file(session, "media")
        media_stale = (
            args.force
            or not media_out.exists()
            or (debate_mtime and debate_mtime > media_out.stat().st_mtime)
        )
        if media_stale:
            logger.info(f"[{session}] parsing media")
            doc = parse_media_for_session(config, session)
            media_out.parent.mkdir(parents=True, exist_ok=True)
            media_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {media_out.name} ({len(doc['data'])} media records)")


def _merge(config, session, args):
    """SE's merge_session returns a dict, not a path; caller writes the file."""
    doc = merge_session(config, session)
    merged_file = config.file(session, "merged", create=True)
    merged_file.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    return merged_file


def _align(config, session, args):
    """Riksdag publishes one MP3 per debate (~40 min, 5-40 speeches); slice
    into per-speech MP3s at the cache paths align_audio expects, then run
    alignment in-memory and write the result manually."""
    merged_file = config.file(session, "merged")
    if not merged_file.exists():
        raise FileNotFoundError(f"[{session}] no merged file - cannot align")

    logger.info(f"[{session}] preparing per-speech audio slices")
    doc = json.loads(merged_file.read_text())
    prepare_per_speech_audio(doc["data"], args.cache_dir, force=args.force)

    logger.info(f"[{session}] running aeneas alignment ({args.aeneas_language})")
    align_audio(
        doc["data"],
        language=args.aeneas_language,
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
    parser.add_argument("--session", dest="protokoll", action="append", default=[],
                        help="Protokoll dok_id to download (e.g. HD0991). May be passed multiple times.")
    parser.add_argument("--limit-anforanden", type=int, default=None,
                        help="Stop the per-speech walk after this many speeches (testing only)")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="SE Riksdag workflow: download → parse → merge → align → NEL → NER.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
