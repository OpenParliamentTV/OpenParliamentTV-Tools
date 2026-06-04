#! /usr/bin/env python3
"""Fetch DE-SN plenary-video list pages into per-Sitzung raw media manifests.

The Saxony mediathek archive is a single paginated list scoped to a Wahlperiode
(``electoral_term_id={wp}``). Each list item is fully self-contained (see
``scraper/common.parse_list_page``), so one pagination pass yields every
per-speech field — there is no separate session-page or per-speech offset GET.

Items are strictly newest-first and a Sitzung's speeches are contiguous in the
listing, so a targeted single-session fetch can stop as soon as it has paged
past the requested Sitzung. Writes ``original/media/{session_id}-raw.json`` per
session, where ``session_id = {wp:02d}{sitzung:03d}`` (e.g. ``08025``):

    {
      "session_id": "08025", "wp": 8, "sitzung": 25, "date": "2026-02-05",
      "speeches": [ {id, date, time, top_no, thema, speaker_raw, faction_raw,
                     speech_type, smil_url, start_offset, end_offset,
                     source_page, tagesordnung_url}, ... ]
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
    __package__ = "optv.parliaments.DE-SN.scraper"

from .common import PAGE_SIZE, fetch_text, list_page_url, parse_list_page

logger = logging.getLogger(__name__)


def _session_id(wp: int, sitzung: int) -> str:
    return f"{wp:02d}{sitzung:03d}"


def fetch_media(*, media_dir: Path, period: int, limit_session: str | None = None,
                force: bool = False, retry_count: int = 20,
                max_pages: int = 400) -> list[str]:
    """Paginate the WP ``period`` archive into per-session raw manifests.

    Returns the list of session_ids written. When ``limit_session`` is given
    (an exact id or a regex prefix), pagination stops once that session has been
    fully collected; otherwise the whole WP archive is walked.
    """
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)

    buckets: dict[str, list[dict]] = {}
    target_seen = False

    for page_no in range(max_pages):
        start = page_no * PAGE_SIZE
        url = list_page_url(str(period), start)
        logger.info(f"Fetching page {page_no + 1} (start={start})")
        html = fetch_text(url, retry_count=retry_count)
        records = parse_list_page(html, wp=period)
        if not records:
            logger.info("Empty page — end of archive reached")
            break

        page_has_target = False
        for rec in records:
            if rec.get("sitzung") is None:
                continue
            sid = _session_id(period, rec["sitzung"])
            buckets.setdefault(sid, []).append(rec)
            if limit_session and re.match(limit_session, sid):
                target_seen = True
                page_has_target = True

        # Targeted fetch: the target Sitzung's speeches are contiguous, so once
        # we have seen it and a later page no longer contains it, we are done.
        if limit_session and target_seen and not page_has_target:
            logger.info("Paged past the requested session — stopping")
            break

    written: list[str] = []
    for sid, speeches in sorted(buckets.items()):
        if limit_session and not re.match(limit_session, sid):
            continue
        out = media_dir / f"{sid}-raw.json"
        if out.exists() and not force:
            logger.info(f"{sid}: raw.json exists — skipping (use --force to refetch)")
            written.append(sid)
            continue
        speeches.sort(key=lambda s: (s.get("date") or "", s.get("start_offset") or 0))
        dates = sorted({s["date"] for s in speeches if s.get("date")})
        payload = {
            "session_id": sid,
            "wp": period,
            "sitzung": speeches[0]["sitzung"],
            "date": dates[0] if dates else None,
            "dates": dates,
            "speeches": speeches,
        }
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        logger.info(f"Wrote {out.name} ({len(speeches)} speeches, {dates})")
        written.append(sid)
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=8)
    parser.add_argument("--limit-session", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--max-pages", type=int, default=400)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    fetch_media(
        media_dir=args.data_dir / "original" / "media",
        period=args.period,
        limit_session=args.limit_session,
        force=args.force,
        retry_count=args.retry_count,
        max_pages=args.max_pages,
    )
