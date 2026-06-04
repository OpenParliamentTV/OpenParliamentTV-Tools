#! /usr/bin/env python3
"""Fetch + parse DE-NW session video pages into per-Sitzung item manifests.

For each candidate session in the archive, GET the mediathek video page
(``/home/mediathek/video.html?kid={kid}``), parse its static ``TEST-REDNER``
spine (``scraper/common.py:parse_video_page``), and derive the authoritative
session id from the page's own ``<h2>{N}. Plenarsitzung</h2>`` header. Then, for
each speech, fetch the redner-selected page (``…&top-redner-id={id}``) once to
read the precise start offset in seconds (``parse_offset``) — the base page only
exposes minute-resolution display times. Candidate pages that 404 are skipped.

Writes ``original/media/{session_id}-items.json`` per session::

    {
      "session_id": "18117", "wp": 18, "sitzung": 117, "date": "2026-01-30",
      "session_start_iso": "2026-01-30T10:00:00+02:00", "kid": "16904f0f-…",
      "video_page_url": "…?kid=16904f0f-…",
      "speeches": [
        {"top_redner_id","mdl_id","funktion_id","name","fraktion","funktion",
         "top_nr","top_title","start_offset","rendered_end","display_time"},
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
    __package__ = "optv.parliaments.DE-NW.scraper"

from .common import fetch_text, parse_offset, parse_video_page, video_page_url

logger = logging.getLogger(__name__)


def _fetch_offset(kid: str, redner_id: str, retry_count: int) -> tuple[int | None, int | None]:
    try:
        html = fetch_text(video_page_url(kid, redner_id), retry_count=retry_count)
    except RuntimeError as ex:
        logger.warning(f"  redner {redner_id}: offset fetch failed ({ex})")
        return None, None
    if html is None:
        return None, None
    return parse_offset(html)


def fetch_media_for_archive(*, archive: dict, media_dir: Path,
                            force: bool = False, retry_count: int = 20,
                            session_filter: str | None = None) -> None:
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)
    period = int(archive.get("wp"))

    for entry in archive.get("sessions", []):
        kid = entry["kid"]
        # If the archive already knows the session id, honour the filter early
        # (saves a page fetch); otherwise we filter after deriving it.
        known_id = entry.get("session_id")
        if session_filter and known_id and not re.match(session_filter, known_id):
            continue
        if known_id:
            out = media_dir / f"{known_id}-items.json"
            if out.exists() and not force:
                logger.info(f"{known_id}: items.json exists — skipping (use --force)")
                continue

        url = video_page_url(kid)
        logger.info(f"Fetching {url}")
        try:
            html = fetch_text(url, retry_count=retry_count)
        except RuntimeError as ex:
            logger.warning(f"{url}: fetch failed ({ex}) — skipping")
            continue
        if html is None:
            logger.debug(f"{kid}: page 404 — skipping")
            continue
        doc = parse_video_page(html)
        if doc is None or not doc.get("sitzung"):
            logger.warning(f"{url}: no parseable session spine — skipping")
            continue

        sitzung = int(doc["sitzung"])
        session_id = f"{period:02d}{sitzung:03d}"
        if session_filter and not re.match(session_filter, session_id):
            logger.debug(f"{session_id}: filtered out — skipping")
            continue
        out = media_dir / f"{session_id}-items.json"
        if out.exists() and not force:
            logger.info(f"{session_id}: items.json exists — skipping (use --force)")
            continue

        start_iso = doc.get("session_start_iso") or ""
        date = start_iso[:10] if start_iso else ""
        speeches = doc["speeches"]
        logger.info(f"{session_id}: {len(speeches)} speeches — fetching precise offsets")
        for sp in speeches:
            rid = sp.get("top_redner_id")
            start, end = (_fetch_offset(kid, rid, retry_count)
                          if rid else (None, None))
            sp["start_offset"] = start
            sp["rendered_end"] = end

        payload = {
            "session_id": session_id,
            "wp": period,
            "sitzung": sitzung,
            "date": date,
            "session_start_iso": start_iso,
            "kid": kid,
            "video_page_url": url,
            "speeches": speeches,
        }
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        logger.info(f"Wrote {out.name} ({len(speeches)} speeches, {date})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=18)
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
