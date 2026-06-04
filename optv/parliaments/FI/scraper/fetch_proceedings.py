#! /usr/bin/env python3
"""Download the verbatim PTK plenary-minutes XML for FI sessions.

The text comes from the avoindata ``VaskiData`` document store (CC-BY), keyed
by ``Eduskuntatunnus`` (``"PTK {number}/{year} vp"``). This deliberately
bypasses the bot-protected ``eduskunta.fi/FI/vaski/...`` web pages — the same
minutes are served, CAPTCHA-free, through the open-data API.

Writes ``original/proceedings/{session}-ptk.xml``. Idempotent: only fetches
what is missing unless ``--force``.
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
    __package__ = "optv.parliaments.FI.scraper"

from optv.parliaments.FI.common import Config, parse_session_str, session_str
from optv.parliaments.FI.scraper.avoindata import fetch_ptk_xml

logger = logging.getLogger(__name__)


def fetch_proceedings(config: Config, year: int, number: int, *,
                      force: bool = False, retry_count: int = 5,
                      retry_delay_max: float = 10.0) -> Path | None:
    session = session_str(year, number)
    out = config.raw_ptk(session)
    if out.exists() and not force:
        logger.info(f"[{session}] ptk.xml exists — skipping (use --force)")
        return out
    xml = fetch_ptk_xml(number, year, retry_count=retry_count,
                        retry_delay_max=retry_delay_max)
    if not xml:
        logger.warning(f"[{session}] no PTK document in VaskiData yet — skipping")
        return None
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(xml, encoding="utf-8")
    logger.info(f"[{session}] wrote {out.name} ({len(xml)} bytes)")
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 2026-058")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    year, number = parse_session_str(args.session)
    config = Config(args.data_dir)
    fetch_proceedings(config, year, number, force=args.force)


if __name__ == "__main__":
    main()
