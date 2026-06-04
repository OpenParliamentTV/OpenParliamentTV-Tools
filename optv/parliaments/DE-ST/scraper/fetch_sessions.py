#! /usr/bin/env python3
"""Download per-Sitzungsperiode HTML pages.

``fetch_archive`` already pulls and caches the SP page during the archive
walk (so it can count day-sections). This module is a thin re-download pass
that honours ``--force`` and fills any gaps. It is a no-op when nothing is
missing.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.scraper"

from .common import LANDTAG_BASE, fetch_text
from .fetch_archive import _sp_page_path

logger = logging.getLogger(__name__)


def fetch_session_pages(
    *,
    sitzung_map: dict,
    proceedings_dir: Path,
    force: bool = False,
    retry_count: int = 10,
) -> list[Path]:
    """Ensure every Sitzungsperiode in the map has a local HTML cache."""
    proceedings_dir = Path(proceedings_dir)
    proceedings_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for entry in sitzung_map.get("sitzungsperioden", []):
        sp = entry["sp"]
        path = _sp_page_path(proceedings_dir, sp)
        if path.exists() and not force:
            paths.append(path)
            continue
        url = f"{LANDTAG_BASE}/{sp}-sitzungsperiode"
        logger.info(f"Fetching {url}")
        html = fetch_text(url, retry_count=retry_count)
        path.write_text(html, encoding="utf-8")
        paths.append(path)
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    from .fetch_archive import load_sitzung_map
    sitzung_map = load_sitzung_map(args.data_dir / "metadata")
    fetch_session_pages(
        sitzung_map=sitzung_map,
        proceedings_dir=args.data_dir / "original" / "proceedings",
        force=args.force,
        retry_count=args.retry_count,
    )
