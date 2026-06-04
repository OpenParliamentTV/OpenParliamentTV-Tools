#! /usr/bin/env python3
"""NO Stortinget workflow.

Downloads `eksport/moter` per session-year, then per meeting fetches the
`publikasjon` XML (proceedings) and the Qbrick media metadata. Parses both
into intermediate JSON, joins on `sak_nummer` + speech order in the
merger, and slices the per-meeting MP4 into per-speech MP3s at align time.

Period semantics: ``--period`` is the OPTV-internal Stortingsperiode index
(see ``common.TERM_TO_PERIOD``). Session strings are ``{period}_{moteid}``
e.g. ``22_11518``. ``--limit-to-period`` filters by the ``22_`` prefix.

NEL needs ``metadata/entities.json`` built locally by
``scraper/build_entity_dump.py`` (no ``no.openparliament.tv`` host yet).
NER needs an Entity-Fishing endpoint with the Norwegian KB loaded.
"""

import datetime as _dt
import json
import logging
import os
import re
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.NO.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.align import align_audio
from optv.shared.workflow import WorkflowHooks, run_main

from .align_prep import prepare_per_speech_audio
from .common import Config, period_to_sesjonider
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_for_meeting
from .parsers.proceedings2json import parse_proceedings_for_meeting
from .scraper.fetch_meetings import fetch_meetings
from .scraper.fetch_media import fetch_media_for_meeting
from .scraper.fetch_proceedings import fetch_proceedings_for_meeting

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _session_in_scope(args, session):
    """NO sessions are ``{period}_{moteid}``. The underscore avoids matching
    period=22 against e.g. ``225_…``."""
    if args.limit_to_period and not session.startswith(f"{args.period}_"):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


def _selected_meetings(args, config: Config) -> list[int]:
    """Resolve which moteids to operate on for the download / parse stages.

    Order of precedence:
      1. ``--meid`` (one or more explicit meeting IDs)
      2. otherwise, every meeting in the relevant sesjonider whose
         ``referat_id`` is non-null (no referat → no proceedings).

    Requires that ``fetch_meetings`` has already written the per-sesjon
    overview JSON; the download stage runs it first so this is a no-op for
    the typical happy path.
    """
    if args.meid:
        return list(args.meid)
    sesjonider = args.sesjon or period_to_sesjonider(args.period)
    moteids: list[int] = []
    for sesjonid in sesjonider:
        path = config.dir("meetings") / f"{sesjonid}.json"
        if not path.exists():
            logger.warning(f"No meetings file for {sesjonid}: {path} - run --download-original first")
            continue
        doc = json.loads(path.read_text())
        for m in doc.get("moter_liste") or []:
            mid = m.get("id")
            if mid is None or mid < 0 or not m.get("referat_id"):
                continue
            moteids.append(mid)
    return moteids


def _download(config: Config, args):
    sesjonider = args.sesjon or period_to_sesjonider(args.period)
    # 1. moter overview per session-year
    for sesjonid in sesjonider:
        logger.info(f"Fetching moter for sesjon {sesjonid}")
        fetch_meetings(config, sesjonid, force=args.force,
                       retry_count=args.retry_count,
                       retry_delay_max=args.retry_delay_max)
    # 2. per-meeting downloads
    meetings = _selected_meetings(args, config)
    if not meetings:
        logger.warning("No meetings selected for download (empty sesjon overview or --meid mismatch)")
        return
    for moteid in meetings:
        logger.info(f"Fetching proceedings + media for moteid={moteid}")
        try:
            fetch_proceedings_for_meeting(config, moteid, force=args.force,
                                          retry_count=args.retry_count,
                                          retry_delay_max=args.retry_delay_max)
            fetch_media_for_meeting(config, moteid, force=args.force,
                                    retry_count=args.retry_count,
                                    retry_delay_max=args.retry_delay_max)
        except Exception as e:
            logger.error(f"[{moteid}] download failed: {type(e).__name__}: {e}")


def _parse(config: Config, args):
    """Per-meeting parse. Re-runs only when an original file is newer than
    the cache file it produces."""
    meetings = _selected_meetings(args, config)
    if not meetings:
        return
    for moteid in meetings:
        session = f"{args.period}_{moteid}"

        proc_in = config.dir("proceedings") / f"{moteid}.xml"
        proc_out = config.file(session, "proceedings")
        proc_stale = (
            args.force
            or (proc_in.exists() and (not proc_out.exists()
                                      or proc_in.stat().st_mtime > proc_out.stat().st_mtime))
        )
        if proc_stale and proc_in.exists():
            logger.info(f"[{session}] parsing proceedings")
            try:
                doc = parse_proceedings_for_meeting(config, args.period, moteid,
                                                   spacy_model=args.spacy_model)
            except Exception as e:
                logger.error(f"[{session}] proceedings parse failed: {type(e).__name__}: {e}")
                continue
            proc_out.parent.mkdir(parents=True, exist_ok=True)
            proc_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {proc_out.name} ({len(doc['data'])} speeches)")

        media_in = config.dir("media") / f"{moteid}-raw.json"
        media_out = config.file(session, "media")
        media_stale = (
            args.force
            or (media_in.exists() and (not media_out.exists()
                                       or media_in.stat().st_mtime > media_out.stat().st_mtime))
        )
        if media_stale and media_in.exists():
            logger.info(f"[{session}] parsing media")
            try:
                doc = parse_media_for_meeting(config, args.period, moteid)
            except Exception as e:
                logger.error(f"[{session}] media parse failed: {type(e).__name__}: {e}")
                continue
            media_out.parent.mkdir(parents=True, exist_ok=True)
            media_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {media_out.name}")


def _merge(config, session, args):
    doc = merge_session(config, session)
    merged_file = config.file(session, "merged", create=True)
    merged_file.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    return merged_file


def _align(config, session, args):
    """One MP4 per meeting part, sliced into per-speech MP3s for aeneas."""
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
    parser.add_argument("--session", dest="sesjon", action="append", default=[],
                        help="Limit work to one or more sesjon-id (e.g. 2025-2026). "
                             "Defaults to all sesjoner of the --period.")
    parser.add_argument("--meid", action="append", type=int, default=[],
                        help="Limit work to specific Storting meeting IDs.")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="NO Stortinget workflow: download → parse → merge → align → NEL → NER.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
