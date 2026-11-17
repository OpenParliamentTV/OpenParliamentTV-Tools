#! /usr/bin/env python3
"""Fetch the publikasjon XML (Referat) for one Storting meeting.

Looks up the meeting's ``referat_id`` from the cached
``original/meetings/{sesjonid}.json`` overview, then downloads the
``eksport/publikasjon`` XML. Despite the API accepting ``format=json`` it
always returns XML for ``refs-...`` ids; the parser consumes that directly.

A Referat passes through the editorial states ``Arbeid`` → ``Kvalitetssikret``
→ ``Komplett`` (root ``Forhandlinger/@Status``). Only a ``Komplett`` file is
served from the cache; any other state is refetched on every run and rewritten
only when its bytes changed, so an unchanged Referat does not re-trigger the
mtime-driven parse.

Writes ``original/proceedings/{moteid}.xml``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.scraper"

from optv.parliaments.NO.common import Config
from optv.parliaments.NO.scraper.http_get import fetch_bytes, strip_bom

logger = logging.getLogger(__name__)

API_ROOT = "https://data.stortinget.no/eksport"
FINAL_STATUS = "Komplett"
_STATUS_RE = re.compile(rb"<Forhandlinger\b[^>]*\bStatus=\"([^\"]*)\"")


def referat_status(head: bytes) -> str | None:
    """``Forhandlinger/@Status`` read from the first bytes of a Referat XML."""
    m = _STATUS_RE.search(head)
    return m.group(1).decode("utf-8", errors="replace") if m else None


def _cached_status(path: Path) -> str | None:
    with open(path, "rb") as f:
        return referat_status(f.read(2048))


def _find_meeting(config: Config, moteid: int) -> dict | None:
    """Search every cached meetings file for the given moteid."""
    for path in config.dir("meetings", create=True).glob("*.json"):
        doc = json.loads(path.read_text())
        for m in doc.get("moter_liste") or []:
            if m.get("id") == moteid:
                return m
    return None


def fetch_proceedings_for_meeting(config: Config, moteid: int, *,
                                  force: bool = False,
                                  retry_count: int = 10,
                                  retry_delay_max: float = 10.0) -> Path | None:
    target = config.dir("proceedings", create=True) / f"{moteid}.xml"
    if target.exists() and not force:
        status = _cached_status(target)
        if status == FINAL_STATUS:
            logger.info(f"[{moteid}] proceedings cache hit: {target.name}")
            return target
        logger.info(f"[{moteid}] cached Referat has status {status!r} — refetching")
    meeting = _find_meeting(config, moteid)
    if not meeting:
        logger.error(f"[{moteid}] no meetings overview entry — run fetch_meetings first")
        return None
    referat_id = meeting.get("referat_id")
    if not referat_id:
        logger.info(f"[{moteid}] meeting has no referat_id — skipping (likely procedural-only)")
        return None
    url = f"{API_ROOT}/publikasjon?publikasjonid={referat_id}"
    logger.info(f"[{moteid}] GET {url}")
    body = strip_bom(fetch_bytes(url, accept="application/xml,application/json;q=0.5,*/*;q=0.1",
                                 timeout=120, retry_count=retry_count,
                                 retry_delay_max=retry_delay_max))
    if target.exists() and target.read_bytes() == body:
        logger.info(f"[{moteid}] {target.name} unchanged (status {referat_status(body[:2048])!r})")
        return target
    target.write_bytes(body)
    logger.info(f"[{moteid}] wrote {target.name} ({len(body)} bytes, "
                f"status {referat_status(body[:2048])!r})")
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--meid", type=int, required=True)
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    fetch_proceedings_for_meeting(config, args.meid, force=args.force,
                                  retry_count=args.retry_count,
                                  retry_delay_max=args.retry_delay_max)


if __name__ == "__main__":
    main()
