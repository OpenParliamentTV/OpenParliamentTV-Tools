#! /usr/bin/env python3
"""Normalize av.parlamento.pt per-meeting JSON into the intermediate media format.

Input:  ``original/media/{session}-av.json`` (raw API JSON; fetch_media.py),
        optional sidecars ``{session}-av-metadata.json`` (per-intervention
        summaries), ``{session}-av-sources.json`` (the recording URLs from the
        meeting page) and ``original/opendata/{session}-od.json`` (open data
        Intervenções records; fetch_opendata.py)
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
- ``summary`` — the av one-line summary of the intervention (agenda assignment).
- ``od`` — the open data record, when there is one: Diário ``pages``, the
  deputy's ``personID`` (``idCadastro``), ``debateID`` / ``debate`` / ``subject``.

The recording path comes from the meeting page's ``meetingVideoSources``; the
path built from ``eventDate`` (``_content_path``) is the fallback. ``eventDate``
is Lisbon local time without an offset, so ``dateStart`` is localised to
Europe/Lisbon.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from optv.parliaments.PT.common import Config, parse_session

logger = logging.getLogger(__name__)

AV_BASE = "https://av.parlamento.pt"
PARLIAMENT_CODE = "PT"
LISBON = ZoneInfo("Europe/Lisbon")

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


def localize(event_date: str) -> Optional[str]:
    """``2026-02-25T14:58:00`` (Lisbon wall clock) → ``2026-02-25T14:58:00+00:00``."""
    if not re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", event_date or ""):
        return None
    try:
        dt = datetime.datetime.fromisoformat(event_date)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LISBON)
    return dt.isoformat()


def _recording_base(sources: Optional[dict], leg: int, sl: int, meeting: int,
                    event_date: str) -> str:
    """``https://av.parlamento.pt/content/hls/…/{file}.mp4`` (no ``/index.m3u8``)."""
    hls = (sources or {}).get("hls") or ""
    if hls.endswith("/index.m3u8"):
        return hls[:-len("/index.m3u8")]
    content_path = _content_path(leg, sl, meeting, event_date)
    return f"{AV_BASE}{content_path}" if content_path else ""


def parse_av(av_doc: dict, session: str, *, metadata: Optional[list] = None,
             sources: Optional[dict] = None, od: Optional[dict] = None) -> dict:
    leg, sl, meeting = parse_session(session)
    event_date = av_doc.get("eventDate") or ""
    base = _recording_base(sources, leg, sl, meeting, event_date)
    session_hls = f"{base}/index.m3u8" if base else ""
    date_start = localize(event_date)
    summaries = {str(x.get("number")): (x.get("title") or "").strip()
                 for x in (metadata or []) if isinstance(x, dict)}
    od_by_number = (od or {}).get("interventions") or {}

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
        if base and start_s is not None and end_s is not None:
            start_ms = int(round(start_s * 1000))
            end_ms = int(round(end_s * 1000))
            video_uri = f"{base}/ClipFrom/{start_ms}/ClipTo/{end_ms}/index.m3u8"

        affiliation = iv.get("affiliation") or {}
        item = {
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
            "summary": summaries.get(str(number), ""),
        }
        if str(number) in od_by_number:
            item["od"] = od_by_number[str(number)]
        items.append(item)

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
            "openData": bool(od_by_number),
            "processing": {
                "parse_media": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": items,
    }


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _inputs(config: Config, session: str) -> list[Path]:
    return [config.raw_av(session), config.raw_av_metadata(session),
            config.raw_av_sources(session), config.raw_od(session)]


def parse_media_for_session(config: Config, session: str) -> dict:
    av_path = config.raw_av(session)
    if not av_path.exists():
        raise FileNotFoundError(f"[{session}] av JSON missing: {av_path}")
    av_doc = json.loads(av_path.read_text(encoding="utf-8"))
    return parse_av(av_doc, session,
                    metadata=_read_json(config.raw_av_metadata(session)),
                    sources=_read_json(config.raw_av_sources(session)),
                    od=_read_json(config.raw_od(session)))


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
        newest = max(p.stat().st_mtime for p in _inputs(config, session) if p.exists())
        if out.exists() and not args.force and out.stat().st_mtime > newest:
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
