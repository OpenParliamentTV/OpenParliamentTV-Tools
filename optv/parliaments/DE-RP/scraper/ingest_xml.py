#! /usr/bin/env python3

# Ingest ePP "Basisdokument" XML files from a delivery inbox into the
# canonical original/proceedings/ layout consumed by the parser.
#
# ePP XML files are delivered to this parliament via a private channel
# rather than fetched from a public URL, so the "scraper" in DE-RP is
# really an inbox watcher: it copies/renames source files into the
# session-keyed naming convention used downstream.
#
# Source filename: ePP_Basisdokument_<WP>-<S>_<DD-MM-YYYY>.xml
# Target filename: <WP><SSS>-proceedings.xml   (e.g. 18077-proceedings.xml)
#
# Idempotent: a content-hash sidecar lets us skip re-copying files that
# haven't changed. A fresh delivery of the same session (e.g. a corrected
# protocol) overwrites the target and bumps mtime so downstream stages re-run.

from __future__ import annotations

import argparse
from hashlib import blake2b
import logging
from pathlib import Path
import re
import shutil
import sys

logger = logging.getLogger(__name__)

EPP_RE = re.compile(r"^ePP_Basisdokument_(\d+)-(\d+)_\d{2}-\d{2}-\d{4}\.xml$",
                    re.IGNORECASE)


def _hash(path: Path) -> str:
    h = blake2b(digest_size=16)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_epp_filename(name: str) -> tuple[str, str] | None:
    """Return (period, session) from an ePP filename, or None if it doesn't match."""
    m = EPP_RE.match(name)
    if not m:
        return None
    return m.group(1), m.group(2)


def session_id(period: str, session: str) -> str:
    return f"{period}{str(session).zfill(3)}"


def ingest_inbox(inbox_dir: Path, proceedings_dir: Path) -> dict:
    """Copy unseen/changed ePP XMLs from inbox/ into original/proceedings/.

    Returns a stats dict {ingested, skipped, ignored}.
    """
    inbox_dir = Path(inbox_dir)
    proceedings_dir = Path(proceedings_dir)
    proceedings_dir.mkdir(parents=True, exist_ok=True)

    stats = {"ingested": 0, "skipped": 0, "ignored": 0}
    if not inbox_dir.is_dir():
        logger.warning(f"Inbox directory does not exist: {inbox_dir}")
        return stats

    for src in sorted(inbox_dir.glob("ePP_Basisdokument_*.xml")):
        parsed = parse_epp_filename(src.name)
        if parsed is None:
            logger.debug(f"Ignoring (unrecognized filename): {src.name}")
            stats["ignored"] += 1
            continue
        period, session = parsed
        sid = session_id(period, session)
        target = proceedings_dir / f"{sid}-proceedings.xml"
        sidecar = proceedings_dir / f"{sid}-proceedings.sha"

        src_hash = _hash(src)
        if target.exists() and sidecar.exists() and sidecar.read_text().strip() == src_hash:
            logger.debug(f"Already ingested: {src.name} -> {target.name}")
            stats["skipped"] += 1
            continue

        logger.info(f"Ingesting {src.name} -> {target.name}")
        shutil.copyfile(src, target)
        sidecar.write_text(src_hash)
        stats["ingested"] += 1

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest ePP XML proceedings from inbox.")
    parser.add_argument("--inbox-dir", type=str, required=True,
                        help="Directory containing ePP_Basisdokument_*.xml files")
    parser.add_argument("--proceedings-dir", type=str, required=True,
                        help="Target directory (typically <data>/original/proceedings)")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    stats = ingest_inbox(Path(args.inbox_dir), Path(args.proceedings_dir))
    logger.info(f"Done. {stats}")
