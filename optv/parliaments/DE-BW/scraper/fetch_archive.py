#! /usr/bin/env python3
"""Build the DE-BW session index (the list of session video-page URLs).

Produces ``metadata/archive-wp{N}.json`` — the candidate list that
``fetch_media`` consumes. Each entry is one session video page:

- ``video_page_url`` — the mediathek page carrying the MP4 + chapter list,
- ``sitzung`` — the Sitzung number read from the slug (provisional; the
  authoritative Wahlperiode + date are read from the MP4 URL on the page),
- ``session_id`` — provisional ``{period:02d}{sitzung:03d}`` used only for
  ``--limit-session`` scoping before a page is fetched.

Two sources are merged (deduplicated by URL):

1. the **filterlist** widget, walked end-to-end via ``?offset=`` pagination
   (``fetch_session_urls``) — the full ~1054-item archive across all WPs, then
   scoped to ``period`` by the slug date (so a period-17 build doesn't pull
   WP-18 pages). With ``--max-results`` only the newest N items are fetched
   (incremental updates).
2. **operator-supplied URLs** — ``--session-url`` (repeatable) and/or a
   ``metadata/seed-urls.txt`` file (one URL per line, ``#`` comments allowed) —
   a manual override for anything pagination misses.

The per-session numeric content-ID in the slug is NOT guessable (it 404s on its
own), so pagination is the way to discover the full slug URLs.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-BW.scraper"

from .common import fetch_session_urls, slug_date, slug_sitzung

logger = logging.getLogger(__name__)

# Wahlperiode date ranges (``[start, end)``, ISO). Used to scope the
# all-WP filterlist down to one term by the session slug date. ``None`` end =
# current term. Extend as new terms roll over.
WP_DATE_RANGE: dict[int, tuple[str, str | None]] = {
    16: ("2016-05-01", "2021-05-12"),
    17: ("2021-05-12", "2026-05-12"),
    18: ("2026-05-12", None),
}


def _in_period(iso: str | None, period: int) -> bool:
    rng = WP_DATE_RANGE.get(period)
    if not rng or iso is None:
        # Unknown period or unparseable slug date: keep it; fetch_media will
        # derive the authoritative WP from the MP4 URL and key it correctly.
        return True
    start, end = rng
    if start and iso < start:
        return False
    if end and iso >= end:
        return False
    return True


def _read_seed_file(metadata_dir: Path) -> list[str]:
    seed = metadata_dir / "seed-urls.txt"
    if not seed.exists():
        return []
    urls = []
    for line in seed.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    logger.info(f"Read {len(urls)} seed URL(s) from {seed.name}")
    return urls


def fetch_archive(*, period: int, media_dir: Path, metadata_dir: Path,
                  seed_urls: list[str] | None = None, max_results: int | None = None,
                  force: bool = False, retry_count: int = 20) -> dict:
    """Build (and cache) the candidate session index for ``period``.

    Cached at ``metadata/archive-wp{period}.json``; rebuilt when missing or
    when ``force`` is set. On a fresh build the whole archive is paginated and
    scoped to ``period`` by slug date. Operator-supplied URLs (``seed_urls`` +
    ``seed-urls.txt``) are always merged in.
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    media_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / f"archive-wp{period}.json"

    urls: dict[str, None] = {}
    if out_path.exists() and not force:
        with out_path.open() as f:
            cached = json.load(f)
        for s in cached.get("sessions", []):
            urls.setdefault(s["video_page_url"], None)
        logger.info(f"Loaded {len(urls)} cached session URL(s) from {out_path.name}")

    if force or not urls:
        try:
            for u in fetch_session_urls(retry_count=retry_count, max_results=max_results):
                if _in_period(slug_date(u), period):
                    urls.setdefault(u, None)
            logger.info(f"WP{period}: {len(urls)} session URL(s) after date-scoping")
        except RuntimeError as e:
            logger.warning(f"Filterlist pagination failed ({e}); relying on seed URLs")

    for u in (seed_urls or []):
        urls.setdefault(u.strip(), None)
    for u in _read_seed_file(metadata_dir):
        urls.setdefault(u, None)

    sessions: list[dict] = []
    for u in urls:
        sitzung = slug_sitzung(u)
        if sitzung is None:
            logger.debug(f"Skipping non-session URL: {u}")
            continue
        sessions.append({
            "video_page_url": u,
            "sitzung": sitzung,
            "date": slug_date(u),
            "session_id": f"{period:02d}{sitzung:03d}",
        })

    archive = {
        "wp": period,
        "built": datetime.now().isoformat("T", "seconds"),
        "sessions": sessions,
    }
    out_path.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out_path.name}: {len(sessions)} candidate session(s)")
    return archive


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--session-url", action="append", default=[],
                        help="Session video-page URL to include (repeatable)")
    parser.add_argument("--max-results", type=int, default=None,
                        help="Only paginate the newest N archive items (default: all)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    fetch_archive(
        period=args.period,
        media_dir=args.data_dir / "original" / "media",
        metadata_dir=args.data_dir / "metadata",
        seed_urls=args.session_url,
        max_results=args.max_results,
        force=args.force,
        retry_count=args.retry_count,
    )
