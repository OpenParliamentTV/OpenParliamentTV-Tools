#! /usr/bin/env python3
"""Build the DE-HH session index (the list of session video-page URLs).

Produces ``metadata/archive-wp{N}.json`` — the candidate list that
``fetch_media`` consumes. Each entry is one session video page:

- ``video_page_url`` — ``https://mediathek.buergerschaft-hh.de/sitzung/{WP}/{n}/``,
- ``sitzung`` — the Sitzung number,
- ``session_id`` — ``{WP:02d}{sitzung:03d}`` (e.g. ``23018``).

Unlike DE-BW (unguessable slug content-IDs requiring AJAX pagination), Hamburg's
session URLs are **fully predictable** (``/sitzung/{WP}/{n}/``), so discovery is
a simple ``1..max`` enumeration. The upper bound for the current term is read
from the mediathek landing page (the highest ``/sitzung/{WP}/{n}/`` link); pass
``--max-session`` to bound an older term (or any range) explicitly. Gaps are
harmless — ``fetch_media`` skips a candidate whose page 404s.

Operator-supplied URLs (``--session-url`` / ``metadata/seed-urls.txt``) are
always merged in as a manual override.
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
    __package__ = "optv.parliaments.DE-HH.scraper"

from .common import front_page_max_session, parse_session_ref, session_url

logger = logging.getLogger(__name__)


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
                  seed_urls: list[str] | None = None,
                  max_session: int | None = None,
                  force: bool = False, retry_count: int = 20) -> dict:
    """Build (and cache) the candidate session index for ``period``.

    Cached at ``metadata/archive-wp{period}.json``; rebuilt when missing or when
    ``force`` is set. Candidates are the union of the ``1..max`` enumeration and
    any operator-supplied URLs.
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    media_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / f"archive-wp{period}.json"

    sitzungen: set[int] = set()
    if out_path.exists() and not force:
        with out_path.open() as f:
            cached = json.load(f)
        for s in cached.get("sessions", []):
            sitzungen.add(int(s["sitzung"]))
        logger.info(f"Loaded {len(sitzungen)} cached candidate(s) from {out_path.name}")

    if force or not sitzungen:
        upper = max_session
        if upper is None:
            upper = front_page_max_session(period, retry_count=retry_count)
            if upper:
                logger.info(f"WP{period}: landing page reports up to Sitzung {upper}")
        if upper:
            sitzungen.update(range(1, upper + 1))
        elif not seed_urls:
            logger.warning(
                f"WP{period}: could not determine an upper Sitzung bound "
                f"(no --max-session, landing page yielded nothing) — relying on seeds")

    for u in (seed_urls or []):
        ref = parse_session_ref(u.strip())
        if ref and ref[0] == period:
            sitzungen.add(ref[1])
    for u in _read_seed_file(metadata_dir):
        ref = parse_session_ref(u)
        if ref and ref[0] == period:
            sitzungen.add(ref[1])

    sessions = [
        {
            "video_page_url": session_url(period, nr),
            "period": period,
            "sitzung": nr,
            "session_id": f"{period:02d}{nr:03d}",
        }
        for nr in sorted(sitzungen)
    ]

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
    parser.add_argument("--period", type=int, default=23)
    parser.add_argument("--session-url", action="append", default=[],
                        help="Session video-page URL to include (repeatable)")
    parser.add_argument("--max-session", type=int, default=None,
                        help="Upper Sitzung bound for the enumeration "
                             "(default: read from the landing page)")
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
        max_session=args.max_session,
        force=args.force,
        retry_count=args.retry_count,
    )
