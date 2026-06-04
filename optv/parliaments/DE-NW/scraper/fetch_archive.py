#! /usr/bin/env python3
"""Build the DE-NW session index (the list of session video pages).

Produces ``metadata/archive-wp{N}.json`` — the candidate list that
``fetch_media`` consumes. Each entry is one session, keyed by its opaque
mediathek ``kid`` (session UUID):

- ``kid`` — the session UUID,
- ``video_page_url`` — ``…/home/mediathek/video.html?kid={kid}``,
- ``sitzung`` / ``date`` / ``session_id`` — best-effort from the archive teaser
  card (``fetch_media`` re-derives the authoritative values from each session's
  own video page, which carries an ``<h2>{N}. Plenarsitzung</h2>`` header).

Two discovery paths:

- **Seed kids** (``--kid`` / ``--session-url`` / ``metadata/seed-urls.txt``):
  the listed sessions are recorded directly with no archive paging — the fast
  path for a single test session.
- **Archive pagination**: otherwise the paginated archive
  (``…/archivierte-aufzeichnungen.html?art=plenarsitzung&page=N``) is walked
  ``1..max`` (max read from page 1), and cards are scoped to the Wahlperiode by
  teaser-card date.
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
    __package__ = "optv.parliaments.DE-NW.scraper"

from .common import (
    archive_max_page,
    archive_page_url,
    fetch_text,
    parse_archive_page,
    parse_kid,
    video_page_url,
)

logger = logging.getLogger(__name__)

# WP 18 began with the constituent session on 1 June 2022 (election 15 May 2022).
# Used to scope archive cards (which span every term back to 2014) to the WP.
WP_START_DATE: dict[int, str] = {18: "2022-06-01"}


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


def _entry(period: int, kid: str, sitzung: int | None = None,
           date: str | None = None) -> dict:
    return {
        "kid": kid,
        "video_page_url": video_page_url(kid),
        "period": period,
        "sitzung": sitzung,
        "date": date,
        "session_id": f"{period:02d}{sitzung:03d}" if sitzung else None,
    }


def fetch_archive(*, period: int, media_dir: Path, metadata_dir: Path,
                  seed_urls: list[str] | None = None,
                  max_pages: int | None = None,
                  force: bool = False, retry_count: int = 20) -> dict:
    """Build (and cache) the candidate session index for ``period``.

    Cached at ``metadata/archive-wp{period}.json``; rebuilt when missing or when
    ``force`` is set. Seed kids are always merged in.
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    media_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / f"archive-wp{period}.json"

    by_kid: dict[str, dict] = {}

    # Seed kids (from CLI + seed file) — recorded directly, no paging needed.
    seeds: list[str] = list(seed_urls or []) + _read_seed_file(metadata_dir)
    for s in seeds:
        kid = parse_kid(s)
        if kid:
            by_kid[kid] = _entry(period, kid)

    if out_path.exists() and not force and not seeds:
        with out_path.open() as f:
            cached = json.load(f)
        for s in cached.get("sessions", []):
            by_kid[s["kid"]] = s
        logger.info(f"Loaded {len(by_kid)} cached candidate(s) from {out_path.name}")

    # Paginate the archive only when we have neither a cache nor seeds (or when
    # forced). Seeds alone are enough to drive a targeted single-session run.
    if force or (not by_kid):
        start = WP_START_DATE.get(period)
        html = fetch_text(archive_page_url(1), retry_count=retry_count)
        upper = max_pages or (archive_max_page(html) if html else None) or 1
        logger.info(f"WP{period}: paginating archive over {upper} page(s)")
        for page in range(1, upper + 1):
            page_html = html if page == 1 else fetch_text(
                archive_page_url(page), retry_count=retry_count)
            if not page_html:
                continue
            for card in parse_archive_page(page_html):
                if start and card.get("date") and card["date"] < start:
                    continue   # older Wahlperiode
                by_kid.setdefault(card["kid"], _entry(
                    period, card["kid"], card.get("sitzung"), card.get("date")))

    sessions = sorted(by_kid.values(),
                      key=lambda s: (s.get("sitzung") or 0, s["kid"]))
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
    parser.add_argument("--period", type=int, default=18)
    parser.add_argument("--session-url", action="append", default=[],
                        help="Session video-page URL or kid to include (repeatable)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Upper archive-page bound (default: read from page 1)")
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
        max_pages=args.max_pages,
        force=args.force,
        retry_count=args.retry_count,
    )
