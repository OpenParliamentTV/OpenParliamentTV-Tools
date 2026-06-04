#! /usr/bin/env python3
"""Fetch + parse DE-BW session video pages into per-Sitzung TOP manifests.

For each candidate session in the archive (optionally filtered by a
``--limit-session`` regex against the provisional ``session_id``), GET the
mediathek video page and parse its static ``e-chapterList`` into the raw TOP +
speech structure. The authoritative ``session_id`` is derived from the MP4 URL
on the page (``…/wahlperiode{wp}/…/Aufzeichnung_{nr}_{part}.mp4``), so a page
listed under the wrong provisional period still lands under the right key.

A single calendar-day Sitzung is sometimes split into several sequential video
files (``Aufzeichnung_{nr}_1.mp4``, ``_2.mp4`` …), each its own mediathek page
with its own chapter list (offsets relative to that part's file) and a TOP
numbering that continues across the parts (a debate can straddle the break:
``Fortsetzung TOP N``). We therefore **group the archive's part-cards by
(Sitzung, date)** and fetch all parts into one manifest, ordered by part number.

Writes ``original/media/{session_id}-tops.json`` per session:

    {
      "session_id": "17140", "wp": 17, "sitzung": 140, "date": "2026-02-04",
      "video_page_url": "<part-1 page URL>",
      "parts": [
        {"part": 1,
         "mp4_url": "https://ltbw-stream.babiel.com/.../Aufzeichnung_140_1.mp4",
         "video_page_url": "https://www.landtag-bw.de/de/mediathek/videos/140-…-615632",
         "tops": [ {"index", "title", "description",
                    "speeches": [{"name_raw","meta_raw","start_offset","clock"}]} ]},
        {"part": 2, ...}
      ]
    }
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-BW.scraper"

from .common import fetch_text, parse_video_page

logger = logging.getLogger(__name__)


def _group_by_session(sessions: list[dict]) -> dict[tuple[int, str], list[dict]]:
    """Group archive part-cards by (Sitzung number, slug date).

    Multi-part sessions appear as several cards sharing the same Sitzung + date.
    Falls back to grouping on the video-page URL when the slug date is missing.
    """
    groups: dict[tuple[int, str], list[dict]] = {}
    for s in sessions:
        key = (s.get("sitzung"), s.get("date") or s["video_page_url"])
        groups.setdefault(key, []).append(s)
    return groups


def fetch_media_for_archive(*, archive: dict, media_dir: Path,
                            force: bool = False, retry_count: int = 20,
                            session_filter: str | None = None) -> None:
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)
    groups = _group_by_session(archive.get("sessions", []))

    for (sitzung, _date), entries in groups.items():
        prov_id = entries[0].get("session_id", "")
        if session_filter and not re.match(session_filter, prov_id):
            continue
        # Cheap mtime skip on the provisional id's output (== authoritative for
        # in-period sessions).
        if (media_dir / f"{prov_id}-tops.json").exists() and not force:
            logger.info(f"{prov_id}: tops.json exists — skipping (use --force to refetch)")
            continue

        parts: list[dict] = []
        for e in entries:
            url = e["video_page_url"]
            logger.info(f"Fetching {url}")
            try:
                html = fetch_text(url, retry_count=retry_count)
            except RuntimeError as ex:
                logger.warning(f"{url}: fetch failed ({ex}) — skipping part")
                continue
            doc = parse_video_page(html)
            if doc is None:
                logger.warning(f"{url}: no parseable chapter list — skipping part")
                continue
            parts.append({
                "part": doc["part"],
                "wp": doc["wp"],
                "sitzung": doc["sitzung"],
                "date": doc["date"],
                "mp4_url": doc["mp4_url"],
                "video_page_url": url,
                "tops": doc["tops"],
            })

        if not parts:
            continue
        parts.sort(key=lambda p: p["part"])
        wp = parts[0]["wp"]
        sitzung_no = parts[0]["sitzung"]
        date = parts[0]["date"]
        session_id = f"{wp:02d}{sitzung_no:03d}"
        out = media_dir / f"{session_id}-tops.json"
        payload = {
            "session_id": session_id,
            "wp": wp,
            "sitzung": sitzung_no,
            "date": date,
            "video_page_url": parts[0]["video_page_url"],
            "parts": [{k: p[k] for k in ("part", "mp4_url", "video_page_url", "tops")}
                      for p in parts],
        }
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        n_speeches = sum(len(t["speeches"]) for p in parts for t in p["tops"])
        logger.info(f"Wrote {out.name} ({len(parts)} part(s), {n_speeches} speeches, {date})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--limit-session", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    archive_path = args.data_dir / "metadata" / f"archive-wp{args.period}.json"
    with archive_path.open() as f:
        archive = json.load(f)
    fetch_media_for_archive(
        archive=archive,
        media_dir=args.data_dir / "original" / "media",
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
