#! /usr/bin/env python3
"""Fetch + parse DE-HH session video pages into per-Sitzung item manifests.

For each candidate session in the archive (optionally filtered by a
``--limit-session`` regex against ``session_id``), GET the mediathek page
(``/sitzung/{WP}/{n}/``) and parse its static markup into the agenda-item +
per-speech structure (``scraper/common.py:parse_session_page``).

Unlike DE-BW, a Hamburg Sitzung is a **single page** — there is no multi-part
MP4 split; instead each Tagesordnungspunkt is its own server-side HLS clip on
the one page. Candidate pages that 404 (gaps in the ``1..max`` enumeration) are
skipped silently.

Writes ``original/media/{session_id}-items.json`` per session::

    {
      "session_id": "23018", "wp": 23, "sitzung": 18, "date": "2026-02-11",
      "session_uuid": "c528c03e-…", "video_page_url": "…/sitzung/23/18/",
      "items": [
        {"index": 0, "sid": "94773ec3-…", "top_number": null,
         "title": "Gedenkworte …", "clean_hls": "…/master.m3u8",
         "sign_hls": "…/master.m3u8",
         "speeches": [{"speech_pk","start_offset","duration","name_raw",
                       "function","download_start","download_stop"}]} ,
        ...
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
    __package__ = "optv.parliaments.DE-HH.scraper"

from .common import fetch_text, parse_session_page

logger = logging.getLogger(__name__)


def fetch_media_for_archive(*, archive: dict, media_dir: Path,
                            force: bool = False, retry_count: int = 20,
                            session_filter: str | None = None) -> None:
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)

    for entry in archive.get("sessions", []):
        session_id = entry["session_id"]
        if session_filter and not re.match(session_filter, session_id):
            continue
        out = media_dir / f"{session_id}-items.json"
        if out.exists() and not force:
            logger.info(f"{session_id}: items.json exists — skipping (use --force to refetch)")
            continue

        url = entry["video_page_url"]
        logger.info(f"Fetching {url}")
        try:
            html = fetch_text(url, retry_count=retry_count)
        except RuntimeError as ex:
            logger.warning(f"{url}: fetch failed ({ex}) — skipping")
            continue
        if html is None:
            logger.debug(f"{session_id}: page 404 (gap in enumeration) — skipping")
            continue
        doc = parse_session_page(html)
        if doc is None:
            logger.warning(f"{url}: no parseable agenda items — skipping")
            continue

        payload = {
            "session_id": session_id,
            "wp": int(entry["period"]),
            "sitzung": int(entry["sitzung"]),
            "date": doc["date"],
            "session_uuid": doc["session_uuid"],
            "video_page_url": url,
            "items": doc["items"],
        }
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        n_speeches = sum(len(it["speeches"]) for it in doc["items"])
        logger.info(f"Wrote {out.name} ({len(doc['items'])} items, "
                    f"{n_speeches} speeches, {doc['date']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=23)
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
