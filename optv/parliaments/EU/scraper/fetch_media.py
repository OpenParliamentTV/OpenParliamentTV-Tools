#! /usr/bin/env python3
"""Fetch EU plenary media metadata from the glcloud content-manager.

For each plenary date (``YYYY-MM-DD``), probe the canonical sitting times
against ``control.eup.glcloud.eu/content-manager/content-page/{event-ref}``
and capture the ``<script id="ng-state">`` SSR JSON. One JSON file per day
collects all discovered sittings for that day.

Layout::

    <data_dir>/original/media/raw-{YYYYMMDD}-events.json   # one record per sitting

The SSR JSON has the shape::

    {
      "contentEventKey": {
        "commonId":   "20251008-0900-PLENARY",
        "eventType":  "PLENARY",
        "title":      "Plenary session",
        "startDate":  "2025-10-08T07:02:50.000Z",
        "endDate":    "2025-10-08T19:05:35.000Z",
        "startTime":  1759906970,        # unix epoch
        "endTime":    1759950335,
        "playerUrl":  "https://vod.media.eup.glcloud.eu/.../master.m3u8",
        "languageMapping": [...],
        ...
      }
    }

We persist the full ``contentEventKey`` block; the parser later picks out
what it needs and resolves the EN audio rendition by walking the HLS master.
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)

CONTENT_PAGE_URL = "https://control.eup.glcloud.eu/content-manager/content-page/{event_ref}"

# Most EP plenary sittings start at 09:00 or 15:00 (occasionally 21:00 for
# evening sittings). Probe these times in order; refine later from CRE VOD
# anchors (which give the exact event-refs used by CRE).
CANDIDATE_SITTING_TIMES = ["0900", "1500", "2100", "1800"]

NG_STATE_RE = re.compile(
    r'<script id="ng-state"[^>]*>(.*?)</script>', re.DOTALL
)


def _normalize_date(date_str: str) -> str:
    """Accept YYYY-MM-DD or YYYYMMDD, return YYYYMMDD."""
    s = date_str.replace("-", "")
    if not re.fullmatch(r"\d{8}", s):
        raise ValueError(f"Bad date {date_str!r} (expected YYYY-MM-DD or YYYYMMDD)")
    return s


def _http_get(url: str, retry_count: int = 5, retry_delay_max: float = 10.0) -> bytes | None:
    """GET a URL with exponential backoff. Returns body bytes or None on 404."""
    req = Request(url, headers={"User-Agent": "optv-eu-scraper/0.1"})
    for attempt in range(retry_count + 1):
        try:
            with urlopen(req, timeout=30) as resp:
                return resp.read()
        except HTTPError as e:
            if e.code == 404:
                return None
            if attempt >= retry_count:
                raise
            logger.warning(f"HTTP {e.code} for {url} (attempt {attempt+1}/{retry_count+1})")
        except URLError as e:
            if attempt >= retry_count:
                raise
            logger.warning(f"URLError for {url}: {e} (attempt {attempt+1}/{retry_count+1})")
        time.sleep(min(2 ** attempt, retry_delay_max))
    return None


def _extract_ng_state(html: bytes) -> dict | None:
    """Parse the <script id="ng-state"> JSON blob from the SSR HTML."""
    text = html.decode("utf-8", errors="replace")
    m = NG_STATE_RE.search(text)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        logger.warning(f"ng-state JSON parse error: {e}")
        return None


def fetch_event(event_ref: str, *, retry_count: int = 5) -> dict | None:
    """Fetch the SSR JSON for one event-ref. Returns the ``contentEventKey``
    block (which is the useful per-event metadata), or None if not found or
    invalid."""
    url = CONTENT_PAGE_URL.format(event_ref=event_ref)
    body = _http_get(url, retry_count=retry_count)
    if not body:
        return None
    state = _extract_ng_state(body)
    if not state:
        return None
    event = state.get("contentEventKey")
    if not event or not event.get("commonId"):
        # Page rendered but has no event payload (probably an unknown event-ref).
        return None
    if event.get("eventType") != "PLENARY":
        logger.debug(f"{event_ref}: eventType={event.get('eventType')} (not PLENARY) — skipping")
        return None
    return event


def discover_events_for_date(date_yyyymmdd: str, *, retry_count: int = 5,
                             candidate_times: list[str] | None = None) -> list[dict]:
    """Probe canonical sitting times for a date; return the events that resolved."""
    times = candidate_times or CANDIDATE_SITTING_TIMES
    events = []
    seen_ids = set()
    for hhmm in times:
        event_ref = f"{date_yyyymmdd}-{hhmm}-PLENARY"
        event = fetch_event(event_ref, retry_count=retry_count)
        if event:
            ev_id = event["commonId"]
            if ev_id in seen_ids:
                continue
            seen_ids.add(ev_id)
            logger.info(f"  + {ev_id}: {event.get('title', '?')} "
                        f"({event.get('startDate')} → {event.get('endDate')})")
            events.append(event)
        else:
            logger.debug(f"  - {event_ref}: no event")
    return events


def write_raw(config_dir: Path, date_yyyymmdd: str, events: list[dict]) -> Path:
    """Write the per-day raw events file. Returns the output path."""
    out_dir = config_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    outfile = out_dir / f"raw-{date_yyyymmdd}-events.json"
    payload = {
        "fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date": f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}",
        "events": events,
    }
    outfile.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    return outfile


def download_media(config, args) -> None:
    """Workflow hook entry: fetch media for the dates implied by args.

    Date sources, in priority order:
      1. ``args.eu_date`` (CLI list, repeatable)
      2. ``args.limit_session`` if it looks like YYYY-MM-DD or YYYYMMDD
      3. otherwise: nothing (we don't auto-enumerate the EP calendar yet)
    """
    dates: list[str] = []
    if getattr(args, "eu_date", None):
        dates.extend(_normalize_date(d) for d in args.eu_date)
    elif getattr(args, "limit_session", None):
        candidate = args.limit_session.strip()
        try:
            dates.append(_normalize_date(candidate))
        except ValueError:
            logger.error(f"--limit-session {candidate!r} is not a YYYY-MM-DD/YYYYMMDD date — "
                         "EU scraper needs a date. Pass --eu-date YYYY-MM-DD.")
            return
    if not dates:
        logger.error("No dates to fetch. Pass --eu-date YYYY-MM-DD (repeatable) or "
                     "--limit-session YYYYMMDD.")
        return

    media_dir = config.dir("media", create=True)
    for date in dates:
        outfile = media_dir / f"raw-{date}-events.json"
        if outfile.exists() and not args.force:
            logger.info(f"[{date}] cached → {outfile.name}")
            continue
        logger.info(f"[{date}] probing sittings…")
        events = discover_events_for_date(date, retry_count=args.retry_count)
        if not events:
            logger.warning(f"[{date}] no plenary sittings found on glcloud")
            continue
        path = write_raw(media_dir, date, events)
        logger.info(f"[{date}] wrote {path.name} ({len(events)} sitting(s))")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path, help="Data directory root")
    parser.add_argument("--eu-date", action="append", default=[],
                        help="Plenary date YYYY-MM-DD or YYYYMMDD (repeatable)")
    parser.add_argument("--retry-count", type=int, default=5)
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if the per-day cache exists")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.eu_date:
        parser.error("at least one --eu-date is required")

    media_dir = args.data_dir / "original" / "media"
    media_dir.mkdir(parents=True, exist_ok=True)

    for date_in in args.eu_date:
        date = _normalize_date(date_in)
        outfile = media_dir / f"raw-{date}-events.json"
        if outfile.exists() and not args.force:
            logger.info(f"[{date}] cached → {outfile.name}")
            continue
        events = discover_events_for_date(date, retry_count=args.retry_count)
        if not events:
            logger.warning(f"[{date}] no plenary sittings found")
            continue
        path = write_raw(media_dir, date, events)
        logger.info(f"[{date}] wrote {path.name} ({len(events)} sitting(s))")


if __name__ == "__main__":
    main()
