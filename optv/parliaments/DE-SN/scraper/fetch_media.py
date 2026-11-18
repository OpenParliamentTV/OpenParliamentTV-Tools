#! /usr/bin/env python3
"""Fetch DE-SN plenary-video list pages into per-Sitzung raw media manifests.

The Saxony mediathek archive is a single paginated list scoped to a Wahlperiode
(``electoral_term_id={wp}``). Each list item is fully self-contained (see
``scraper/common.parse_list_page``), so one pagination pass yields every
per-speech field — there is no separate session-page or per-speech offset GET.

Items are sorted by Sitzungsnummer (highest first) and chronologically within a
sitting, so a Sitzung's speeches are contiguous and a targeted single-session
fetch can stop as soon as it has paged past the requested Sitzung.

Each item's Wahlperiode is derived from its date (``common.derive_wp``): the
list filter files WP 8's constituent sitting (2024-10-01) under WP 7, at the end
of the WP 7 list after WP 7's own first sitting. A WP run therefore keeps only
the items whose derived WP matches, and picks up its constituent sitting from
the tail of the previous WP's list (a few pages, walked backwards).

Writes ``original/media/{session_id}-raw.json`` per session, where
``session_id = {wp:02d}{sitzung:03d}`` (e.g. ``08025``):

    {
      "session_id": "08025", "wp": 8, "sitzung": 25, "date": "2026-02-05",
      "speeches": [ {id, wp, date, time, top_no, thema, speaker_raw, faction_raw,
                     speech_type, smil_url, start_offset, end_offset,
                     source_page, tagesordnung_url}, ... ]
    }
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

from .common import (PAGE_SIZE, WP_START_DATES, fetch_text, last_page_start,
                     list_page_url, parse_list_page)

logger = logging.getLogger(__name__)

# Safety cap for the backwards walk over the previous WP's list tail.
MAX_TAIL_PAGES = 20


def _session_id(wp: int, sitzung: int) -> str:
    return f"{wp:02d}{sitzung:03d}"


def _collect_predecessor_tail(period: int, *, retry_count: int = 20) -> list[dict]:
    """Return the items of WP ``period`` that the list files under ``period - 1``.

    The constituent sitting sorts last in the previous WP's list (sitting 1,
    after that WP's own sitting 1), so the walk starts at the last page and goes
    backwards until it reaches an item dated before WP ``period`` began.
    Returns [] when either WP's start date is unknown.
    """
    prev = period - 1
    cutoff = WP_START_DATES.get(period)
    if cutoff is None or prev not in WP_START_DATES:
        return []
    first = fetch_text(list_page_url(str(prev), 0), retry_count=retry_count)
    last = last_page_start(first)
    if last is None:
        return []
    found: list[dict] = []
    start = last
    for _ in range(MAX_TAIL_PAGES):
        if start < 0:
            break
        html = first if start == 0 else fetch_text(
            list_page_url(str(prev), start), retry_count=retry_count)
        records = parse_list_page(html, wp=prev)
        found.extend(r for r in records if r["wp"] == period)
        if not records or any((r.get("date") or "") < cutoff for r in records):
            break
        start -= PAGE_SIZE
    else:
        logger.warning(f"WP {prev} list tail: stopped after {MAX_TAIL_PAGES} pages")
    if found:
        logger.info(f"Picked up {len(found)} WP {period} item(s) filed under WP {prev}")
    return found


def _write_raw(media_dir: Path, sid: str, speeches: list[dict], *,
               force: bool = False) -> bool:
    """Write one session's raw manifest; return False when it exists (no force)."""
    out = Path(media_dir) / f"{sid}-raw.json"
    if out.exists() and not force:
        logger.info(f"{sid}: raw.json exists — skipping (use --force to refetch)")
        return False
    speeches = sorted(speeches, key=lambda s: (s.get("date") or "", s.get("start_offset") or 0))
    dates = sorted({s["date"] for s in speeches if s.get("date")})
    payload = {
        "session_id": sid,
        "wp": speeches[0]["wp"],
        "sitzung": speeches[0]["sitzung"],
        "date": dates[0] if dates else None,
        "dates": dates,
        "speeches": speeches,
    }
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out.name} ({len(speeches)} speeches, {dates})")
    return True


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

    def _add(rec: dict) -> str | None:
        if rec.get("sitzung") is None:
            return None
        sid = _session_id(rec["wp"], rec["sitzung"])
        buckets.setdefault(sid, []).append(rec)
        return sid

    for rec in _collect_predecessor_tail(period, retry_count=retry_count):
        _add(rec)

    target_seen = False
    rekeyed = 0
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
            if rec["wp"] != period:
                rekeyed += 1      # belongs to another WP by date
                continue
            sid = _add(rec)
            if sid and limit_session and re.match(limit_session, sid):
                target_seen = True
                page_has_target = True

        # Targeted fetch: the target Sitzung's speeches are contiguous, so once
        # we have seen it and a later page no longer contains it, we are done.
        if limit_session and target_seen and not page_has_target:
            logger.info("Paged past the requested session — stopping")
            break
    if rekeyed:
        logger.info(f"Skipped {rekeyed} item(s) of the WP {period} list dated in another WP")

    written: list[str] = []
    for sid, speeches in sorted(buckets.items()):
        if limit_session and not re.match(limit_session, sid):
            continue
        _write_raw(media_dir, sid, speeches, force=force)
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
