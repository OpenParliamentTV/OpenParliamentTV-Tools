#! /usr/bin/env python3
"""Normalize av.parlamento.pt per-meeting JSON into the intermediate media format.

Input:  ``original/media/{session}-av.json`` (raw API JSON; fetch_media.py)
Output: ``original/media/{session}-media.json``
        ``{"meta": {...}, "data": [<intervention>, ...]}`` — one media item per
        intervention, in source order. **This is the merge spine** (the av list
        defines which speeches exist); the merger grafts the DAR text onto each.

Per intervention we derive:

- ``videoFileURI`` — the per-speech server-side-clipped HLS stream
  (``…/{session}.mp4/ClipFrom/{startMs}/ClipTo/{endMs}/index.m3u8``), built from
  the JSON ``startTime``/``endTime`` (ms). This is a discrete per-speech clip
  (the platform plays it directly, no ``#t=`` fragment needed).
- ``audioFileURI`` — the un-clipped session HLS (``…/{session}.mp4/index.m3u8``);
  ``align_prep`` downloads it once per session and slices ``[startOffset,
  startOffset+duration]`` for aeneas.
- ``startOffset`` / ``duration`` (seconds) — for align_prep + the merger.
- ``sourcePage`` — the per-speech av page (unique per speech).
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
import sys
from pathlib import Path
from typing import Optional

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.PT.parsers"

from optv.parliaments.PT.common import Config, parse_session

logger = logging.getLogger(__name__)

AV_BASE = "https://av.parlamento.pt"
PARLIAMENT_CODE = "PT"

_HMS_RE = re.compile(r"^(\d+):(\d{2}):(\d{2}(?:\.\d+)?)$")


def _hms_to_seconds(value: Optional[str]) -> Optional[float]:
    """``00:11:21.7085479`` → 681.708… seconds."""
    if not value:
        return None
    m = _HMS_RE.match(value.strip())
    if not m:
        return None
    h, mnt, sec = m.groups()
    return int(h) * 3600 + int(mnt) * 60 + float(sec)


def _content_path(leg: int, sl: int, meeting: int, event_date: str) -> Optional[str]:
    """``/content/hls/DAR/L17/SL1/A2026/M02/2026_02_25_059.mp4`` from the date."""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", event_date or "")
    if not m:
        return None
    yyyy, mm, dd = m.groups()
    fname = f"{yyyy}_{mm}_{dd}_{meeting:03d}.mp4"
    return f"/content/hls/DAR/L{leg}/SL{sl}/A{yyyy}/M{mm}/{fname}"


def parse_av(av_doc: dict, session: str) -> dict:
    leg, sl, meeting = parse_session(session)
    event_date = av_doc.get("eventDate") or ""
    content_path = _content_path(leg, sl, meeting, event_date)
    session_hls = f"{AV_BASE}{content_path}/index.m3u8" if content_path else ""

    date_start = None
    if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", event_date):
        date_start = event_date  # naive local (Europe/Lisbon); kept as-is

    items: list[dict] = []
    for iv in av_doc.get("interventions") or []:
        number = iv.get("number")
        start_s = _hms_to_seconds(iv.get("startTime"))
        end_s = _hms_to_seconds(iv.get("endTime"))
        duration = None
        if start_s is not None and end_s is not None and end_s > start_s:
            duration = round(end_s - start_s, 3)
        elif _hms_to_seconds(iv.get("duration")) is not None:
            duration = round(_hms_to_seconds(iv.get("duration")), 3)

        video_uri = ""
        if content_path and start_s is not None and end_s is not None:
            start_ms = int(round(start_s * 1000))
            end_ms = int(round(end_s * 1000))
            video_uri = (f"{AV_BASE}{content_path}"
                         f"/ClipFrom/{start_ms}/ClipTo/{end_ms}/index.m3u8")

        affiliation = iv.get("affiliation") or {}
        items.append({
            "number": number,
            "interventionType": iv.get("interventionType") or "",
            "speakerType": iv.get("speakerType") or "",
            "speaker": iv.get("speaker") or "",
            "role": iv.get("role") or "",
            "affiliation": {
                "name": affiliation.get("name") or "",
                "initials": affiliation.get("initials") or "",
            },
            "startOffset": round(start_s, 3) if start_s is not None else None,
            "duration": duration,
            "videoFileURI": video_uri,
            "audioFileURI": session_hls,
            "sourcePage": f"{AV_BASE}/videos/Plenary/{leg}/{sl}/{meeting}/{number}",
        })

    return {
        "meta": {
            "session": session,
            "parliament": PARLIAMENT_CODE,
            "title": av_doc.get("title") or "",
            "description": av_doc.get("description") or "",
            "eventDate": event_date,
            "dateStart": date_start,
            "legislature": leg,
            "legislativeSession": sl,
            "meeting": meeting,
            "sessionVideo": session_hls,
            "processing": {
                "parse_media": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": items,
    }


def parse_media_for_session(config: Config, session: str) -> dict:
    av_path = config.raw_av(session)
    if not av_path.exists():
        raise FileNotFoundError(f"[{session}] av JSON missing: {av_path}")
    av_doc = json.loads(av_path.read_text(encoding="utf-8"))
    return parse_av(av_doc, session)


def parse_media_directory(config: Config, args) -> None:
    for session in config.sessions():
        if getattr(args, "pt_session", None) and session not in args.pt_session:
            continue
        if getattr(args, "limit_session", None):
            try:
                if not re.match(args.limit_session, session):
                    continue
            except re.error:
                if args.limit_session != session:
                    continue
        out = config.file(session, "media")
        raw = config.raw_av(session)
        if (out.exists() and not args.force
                and out.stat().st_mtime > raw.stat().st_mtime):
            logger.debug(f"[{session}] media intermediate cached")
            continue
        logger.info(f"[{session}] parsing media")
        doc = parse_media_for_session(config, session)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"[{session}] wrote {out.name} ({len(doc['data'])} interventions)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 17-1-059")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    parse_session(args.session)
    doc = parse_media_for_session(config, args.session)
    out = config.file(args.session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} interventions)")


if __name__ == "__main__":
    main()
