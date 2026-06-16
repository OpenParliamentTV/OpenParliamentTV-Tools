#! /usr/bin/env python3
"""Parse the raw Mediathek payload into per-speech media records (the spine).

Reads ``original/media/{session}-mediathek.json`` (written by the scraper, with
each ``redner`` already carrying a resolved ``video`` block) and emits one media
record per on-camera speech, keyed by ``std_id``. Speeches whose video could not
be resolved are dropped — a media-spine record without a ``videoFileURI`` is not
a valid speech (the platform needs the clip to render it).

Output (intermediate, consumed by the merger)::

    {"meta": {session, period, dateStart, dateEnd, …},
     "data": [{stdId, originMediaID, speakerName, padIntern, debatteId,
               agendaTitle, speechIndex, dateStart, dateEnd, media:{…}}, …]}
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.AT.parsers"

from optv.parliaments import get_rights
from optv.parliaments.AT.common import Config

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))


def _add_seconds(iso: str, seconds) -> str | None:
    if not iso or seconds is None:
        return None
    try:
        base = _dt.datetime.fromisoformat(iso)
    except ValueError:
        return None
    return (base + _dt.timedelta(seconds=float(seconds))).replace(microsecond=0).isoformat()


def parse_media(raw: dict, period: int) -> dict:
    rights = get_rights("AT", period, "media")
    session = raw.get("session") or f"{period}{int(raw.get('sitting', 0)):03d}"
    mediathek_url = raw.get("mediathekURL") or ""
    records: list[dict] = []
    seen: set = set()

    for debatte in raw.get("debatten") or []:
        debatte_id = debatte.get("debatte_id")
        agenda_title = (debatte.get("content") or "").strip()
        for redner in debatte.get("redner") or []:
            std_id = redner.get("std_id")
            video = redner.get("video") or {}
            video_uri = video.get("hls") or video.get("mp4")
            if std_id is None or not video_uri:
                logger.debug(f"[{session}] dropping speech std_id={std_id}: no resolvable video")
                continue
            ts = redner.get("ts")
            # The Mediathek payload sometimes lists the exact same speech twice
            # (identical std_id + ts); keep only the first.
            dedup_key = (std_id, ts)
            if dedup_key in seen:
                logger.debug(f"[{session}] dropping duplicate redner std_id={std_id} ts={ts}")
                continue
            seen.add(dedup_key)
            duration = video.get("duration")
            date_start = redner.get("time")
            date_end = _add_seconds(date_start, duration)

            # sourcePage must be unique per speech (the platform keys speech
            # identity on it). DEBATTE+TS alone collide when two distinct
            # speeches share a timestamp second, so the std_id disambiguates.
            source_page = f"{mediathek_url}?DEBATTE={debatte_id}&TS={ts}&STN={std_id}"

            media: dict = {
                "videoFileURI": video_uri,
                "sourcePage": source_page,
                "creator": rights.get("creator", ""),
                "license": rights.get("license", ""),
                "originMediaID": str(std_id),
            }
            if video.get("mp4"):
                media["videoMP4URI"] = video["mp4"]
            if video.get("mp3"):
                media["audioFileURI"] = video["mp3"]
            if isinstance(duration, (int, float)) and duration >= 0:
                media["duration"] = duration

            records.append({
                "stdId": int(std_id),
                "originMediaID": str(std_id),
                "speakerName": redner.get("name") or "",
                "padIntern": str(redner.get("pad_intern")) if redner.get("pad_intern") is not None else None,
                "debatteId": debatte_id,
                "agendaTitle": agenda_title,
                "dateStart": date_start,
                "dateEnd": date_end,
                "media": media,
            })

    # Sequential speechIndex over the kept records (debatten order).
    for i, rec in enumerate(records, start=1):
        rec["speechIndex"] = i

    date_start = records[0]["dateStart"] if records else None
    date_end = next((r["dateEnd"] for r in reversed(records) if r.get("dateEnd")), None)
    return {
        "meta": {"session": session, "period": period,
                 "dateStart": date_start, "dateEnd": date_end},
        "data": records,
    }


def parse_session(config: Config, session: str, period: int) -> dict:
    raw_path = config.dir("media") / f"{session}-mediathek.json"
    if not raw_path.exists():
        sys.exit(f"Raw Mediathek payload missing: {raw_path}")
    raw = json.loads(raw_path.read_text())
    return parse_media(raw, period)


def main():
    parser = argparse.ArgumentParser(description="Parse AT Mediathek payload into intermediate media JSON.")
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key (e.g. 27144)")
    parser.add_argument("--period", type=int, default=27)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = parse_session(config, args.session, args.period)
    out = config.file(args.session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} media records)")


if __name__ == "__main__":
    main()
