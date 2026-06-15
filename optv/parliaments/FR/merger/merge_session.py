#! /usr/bin/env python3
"""Merge FR proceedings (text spine) with the séance video into Stage 2 JSON.

**Proceedings are the spine.** Each speech already carries its video offset
(``debug.stime``, seconds into the séance recording) from the compte rendu, so
merging is a single ordered walk: attach the séance's one HLS video to every
speech, set ``startOffset = stime`` and ``duration = next_stime − stime``, and
wrap each speech with the ``parliament`` / ``electoralPeriod`` / ``session``
envelope. No cross-source join is needed — unlike the EU/FI media-spine model,
FR's text and offsets come from the same document; only the video URL is
external (resolved per séance via the réunion id, see scraper/fetch_media.py).

Inputs::

    original/proceedings/{session}-proceedings.json   (proceedings parser)
    original/media/{session}-media.json               (media parser; 1 video)

Output: ``cache/merged/{session}-merged.json`` (validates against
``optv/shared/schema/stage2-full.schema.json``).
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import statistics
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.FR.merger"

from optv.parliaments.FR.common import Config, save_if_changed, session_number_int
from optv.parliaments import get_rights as _get_rights
from optv.shared.speech_id import normalize_speech_originid
from optv.shared.meta import build_meta, now_iso

logger = logging.getLogger(__name__)

PARLIAMENT = "FR"
CREATOR = _get_rights("FR", stream="media")["creator"]
LICENSE = _get_rights("FR", stream="media")["license"]
# Fallback duration (s) for the final speech of a séance, which has no following
# stime to bound it. Median of the séance's other speeches, capped, is used.
DEFAULT_LAST_DURATION = 120.0


def _media_block(video: dict, start_offset: float, duration: float,
                 speech_index: int) -> dict:
    hls = video.get("hlsUrl") or ""
    end = start_offset + duration
    source_page = video.get("sourcePage") or hls
    # sourcePage must be unique per speech (semantic validator); the timeCode +
    # speech index make each speech's CRV reference distinct even when two
    # speeches share a stime (e.g. a chair hand-over at the same video moment).
    sep = "&" if "?" in source_page else "?"
    return {
        "videoFileURI": f"{hls}#t={start_offset:.2f},{end:.2f}",
        "sourcePage": f"{source_page}{sep}timeCode={start_offset:.2f}&i={speech_index}",
        "audioFileURI": hls,
        "duration": round(duration, 2),
        "aligned": False,
        "creator": CREATOR,
        "license": LICENSE,
        "originMediaID": video.get("crvId") or "",
        "additionalInformation": {
            "startOffset": round(start_offset, 2),
            "crvId": video.get("crvId") or "",
        },
    }


def _offsets_and_durations(speeches: list[dict]) -> list[tuple[float, float]]:
    """Assign (startOffset, duration) per speech in document order.

    ``startOffset`` is the speech's stime (carried forward when a speech lacks
    one); ``duration`` runs to the next speech's offset, with a median-based
    fallback for the final speech.
    """
    offsets: list[float] = []
    last = 0.0
    for sp in speeches:
        stime = sp.get("debug", {}).get("stime")
        last = float(stime) if stime is not None else last
        offsets.append(last)
    durations: list[float] = []
    for i, off in enumerate(offsets):
        if i + 1 < len(offsets):
            durations.append(max(0.0, offsets[i + 1] - off))
        else:
            durations.append(None)  # filled below
    positive = [d for d in durations if d]
    fallback = min(statistics.median(positive), DEFAULT_LAST_DURATION) if positive else DEFAULT_LAST_DURATION
    durations = [d if d is not None else fallback for d in durations]
    return list(zip(offsets, durations))


def merge_session(session: str, config: Config, args=None) -> Path:
    proc_path = config.file(session, "proceedings")
    media_path = config.file(session, "media")
    if not proc_path.exists():
        raise FileNotFoundError(f"[{session}] proceedings missing: {proc_path}")
    if not media_path.exists():
        raise FileNotFoundError(f"[{session}] media missing: {media_path}")

    proc_doc = json.loads(proc_path.read_text())
    media_doc = json.loads(media_path.read_text())
    video = media_doc.get("data") or {}
    if not video.get("hlsUrl"):
        raise RuntimeError(f"[{session}] media descriptor has no HLS URL")

    speeches = proc_doc.get("data") or []
    if not speeches:
        raise RuntimeError(f"[{session}] no speeches to merge")

    period = getattr(args, "period", None) or 17
    session_number = session_number_int(session)
    meta = proc_doc.get("meta") or {}
    session_start = meta.get("dateStart")
    session_end = meta.get("dateEnd")

    timing = _offsets_and_durations(speeches)
    records: list[dict] = []
    for idx, (sp, (start_offset, duration)) in enumerate(zip(speeches, timing), start=1):
        rec = deepcopy(sp)
        rec.pop("_stime", None)
        rec["parliament"] = PARLIAMENT
        rec["electoralPeriod"] = {"number": int(period)}
        session_obj: dict[str, Any] = {"number": session_number}
        if session_start:
            session_obj["dateStart"] = session_start
        if session_end:
            session_obj["dateEnd"] = session_end
        rec["session"] = session_obj
        rec["speechIndex"] = idx
        rec["media"] = _media_block(video, start_offset, duration, idx)
        if not rec.get("dateStart") and session_start:
            rec["dateStart"] = session_start
        records.append(rec)

    logger.info(f"[{session}] merged {len(records)} speeches onto {video.get('crvId')}")

    now = datetime.datetime.utcnow().isoformat(timespec="seconds")
    for _s in records:
        normalize_speech_originid(_s)
    out_doc = {
        "meta": build_meta(
            PARLIAMENT,
            session=session,
            electoral_period=int(period),
            date_start=session_start,
            date_end=session_end,
            last_update=now,
            processing={
                **(meta.get("processing") or {}),
                **((media_doc.get("meta") or {}).get("processing") or {}),
                "merge": now,
            },
            extra={"sourceLabel": meta.get("sourceLabel", f"Compte rendu {session}")},
        ),
        "data": records,
    }
    out_path = config.file(session, "merged", create=True)
    if save_if_changed(out_doc, out_path):
        logger.info(f"[{session}] wrote {out_path.name}")
    else:
        logger.info(f"[{session}] no content change; left {out_path.name} untouched")
    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("session", type=str, help="Session key, e.g. 2026O1N232")
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    out = merge_session(args.session, config, args)
    print(out)


if __name__ == "__main__":
    main()
