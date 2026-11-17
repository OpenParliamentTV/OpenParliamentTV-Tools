#! /usr/bin/env python3
"""Download each sitting's Plenarprotokoll PDF.

The SP page links every day's protocol (``Stenografischer Bericht``) as
``https://www.landtag.sachsen-anhalt.de/fileadmin/files/plenum/wp{P}/{NNN}stzg.pdf``
once it is published; that address redirects to the PADOKA documentation
server. The protocol states the sitting's clock times (``Beginn``,
``Unterbrechung``/``Wiederbeginn``, ``Schluss``), from which the parser and
merger derive wall-clock speech times.

Files land at ``original/proceedings/{PPNNN}.pdf``. A sitting without a
protocol link in the Sitzung map is skipped; the map picks the link up once
the SP page shows it.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.scraper"

from .common import fetch
from .fetch_archive import session_id

logger = logging.getLogger(__name__)


def protocol_path(proceedings_dir: Path, session: str) -> Path:
    return Path(proceedings_dir) / f"{session}.pdf"


def fetch_protocols_for_session_map(
    *,
    sitzung_map: dict,
    proceedings_dir: Path,
    force: bool = False,
    retry_count: int = 10,
    session_filter: str = "",
) -> None:
    """Fetch the protocol PDF of every in-scope sitting that links one."""
    proceedings_dir = Path(proceedings_dir)
    proceedings_dir.mkdir(parents=True, exist_ok=True)
    period = int(sitzung_map["period"])
    filter_re = re.compile(session_filter) if session_filter else None
    for entry in sitzung_map.get("sitzungsperioden", []):
        for sit in entry["sittings"]:
            session = session_id(period, sit["sitzung"])
            if filter_re and not filter_re.match(session):
                continue
            url = sit.get("protocolURL")
            out = protocol_path(proceedings_dir, session)
            if not url or (out.exists() and not force):
                continue
            logger.info(f"Fetching protocol {session}: {url}")
            try:
                body = fetch(url, retry_count=retry_count)
            except RuntimeError as e:
                logger.warning(f"Protocol {session} not downloaded: {e}")
                continue
            if not body.startswith(b"%PDF"):
                logger.warning(f"Protocol {session}: {url} did not return a PDF")
                continue
            out.write_bytes(body)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=9)
    parser.add_argument("--limit-session", default="")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    from .fetch_archive import load_sitzung_map
    fetch_protocols_for_session_map(
        sitzung_map=load_sitzung_map(args.data_dir / "metadata", args.period),
        proceedings_dir=args.data_dir / "original" / "proceedings",
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
