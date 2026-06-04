#! /usr/bin/env python3
"""Fetch the publikasjon XML (Referat) for one Storting meeting.

Looks up the meeting's ``referat_id`` from the cached
``original/meetings/{sesjonid}.json`` overview, then downloads the
``eksport/publikasjon`` XML. Despite the API accepting ``format=json`` it
always returns XML for ``refs-...`` ids; the parser consumes that directly.

Writes ``original/proceedings/{moteid}.xml``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.scraper"

from optv.parliaments.NO.common import Config

logger = logging.getLogger(__name__)

API_ROOT = "https://data.stortinget.no/eksport"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")


def _fetch_bytes(url: str, *, retry_count: int, retry_delay_max: float) -> bytes:
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            req = Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/xml,application/json;q=0.5,*/*;q=0.1",
            })
            with urlopen(req, timeout=120) as resp:
                return resp.read()
        except HTTPError as e:
            if 500 <= e.code < 600 and attempt < retry_count:
                logger.warning(f"HTTP {e.code} on {url}, retry {attempt}/{retry_count}")
                time.sleep(min(delay, retry_delay_max))
                delay = min(delay * 2, retry_delay_max)
                last_exc = e
                continue
            raise
        except (URLError, TimeoutError, ConnectionError) as e:
            if attempt < retry_count:
                logger.warning(f"{type(e).__name__} on {url}, retry {attempt}/{retry_count}")
                time.sleep(min(delay, retry_delay_max))
                delay = min(delay * 2, retry_delay_max)
                last_exc = e
                continue
            raise
    raise RuntimeError(f"Exhausted {retry_count} attempts for {url}: {last_exc}")


def _strip_bom(b: bytes) -> bytes:
    return b[3:] if b.startswith(b"\xef\xbb\xbf") else b


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
        logger.info(f"[{moteid}] proceedings cache hit: {target.name}")
        return target
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
    body = _strip_bom(_fetch_bytes(url, retry_count=retry_count,
                                   retry_delay_max=retry_delay_max))
    target.write_bytes(body)
    logger.info(f"[{moteid}] wrote {target.name} ({len(body)} bytes)")
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
