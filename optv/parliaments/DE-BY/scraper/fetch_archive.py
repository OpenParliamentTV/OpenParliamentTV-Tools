#! /usr/bin/env python3
"""Discover the DE-BY "Plenum Online" archive structure for a Wahlperiode.

Produces ``metadata/archive-wp{N}.json`` — the session index the downstream
``fetch_media`` / ``media2json`` consume. For each WP-N plenary session it
records:

- ``session_id`` (``19{NNN}`` = WP + citation Sitzungsnr),
- ``date`` (ISO) and the accordion ``gremium_id`` (the ``sitzungGremiumId``
  needed to (re)load the session via JSF),
- ``sitzungsnr`` (canonical Plenarprotokoll number, read from the loaded
  session's Tagesordnung link — it is NOT in the dropdown or the JSON),
- ``tops`` (the Tagesordnungspunkt headers: index + title), so the merger has
  the agenda titles without re-driving the JSF app.

This is the **cheap pass**: one GET (the session dropdown) plus one valueChange
POST per session (loads the TOP headers but not the lazy panel bodies). The
``meta_vod`` playlist URLs live behind a per-TOP tabChange and are fetched by
``fetch_media`` only for the sessions actually being processed.
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
    __package__ = "optv.parliaments.DE-BY.scraper"

from .common import (
    WP_DATE_RANGE,
    PlonSession,
    parse_session_options,
    parse_sitzungsnr,
    parse_tab_headers,
)

logger = logging.getLogger(__name__)


def _iso(de_date: str) -> str | None:
    try:
        return datetime.strptime(de_date, "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def _in_period(iso: str, period: int) -> bool:
    rng = WP_DATE_RANGE.get(period)
    if not rng:
        # Unknown period: accept everything and let the operator notice.
        return True
    start, end = rng
    if start and iso < start:
        return False
    if end and iso >= end:
        return False
    return True


def fetch_archive(*, period: int, media_dir: Path, metadata_dir: Path,
                  force: bool = False, retry_count: int = 20) -> dict:
    """Build (and cache) the session index for ``period``.

    Cached at ``metadata/archive-wp{period}.json``; rebuilt when missing or
    when ``force`` is set.
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    media_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / f"archive-wp{period}.json"

    if out_path.exists() and not force:
        with out_path.open() as f:
            archive = json.load(f)
        logger.info(f"Using cached {out_path.name} ({len(archive.get('sessions', []))} sessions)")
        return archive

    session = PlonSession(retry_count=retry_count)
    accordion_html = session.start()
    options = parse_session_options(accordion_html)
    wanted = [(gid, de_date, iso) for gid, de_date in options
              if (iso := _iso(de_date)) and _in_period(iso, period)]
    logger.info(f"WP{period}: {len(wanted)} of {len(options)} dropdown sessions in range")

    sessions: list[dict] = []
    for gid, de_date, iso in wanted:
        html = session.load_session(gid)
        sitzungsnr = parse_sitzungsnr(html)
        tops = parse_tab_headers(html)
        if sitzungsnr is None:
            logger.warning(f"{iso} (gremium {gid}): no sitzungsnr found — skipping")
            continue
        session_id = f"{period:02d}{sitzungsnr:03d}"
        sessions.append({
            "session_id": session_id,
            "date": iso,
            "gremium_id": gid,
            "sitzungsnr": sitzungsnr,
            "tops": [{"index": t["index"], "title": t["title"]} for t in tops],
        })
        logger.debug(f"{session_id} ({iso}): {len(tops)} TOPs")

    archive = {
        "wp": period,
        "built": datetime.now().isoformat("T", "seconds"),
        "sessions": sessions,
    }
    out_path.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out_path.name}: {len(sessions)} sessions")
    return archive


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=19)
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
        force=args.force,
        retry_count=args.retry_count,
    )
