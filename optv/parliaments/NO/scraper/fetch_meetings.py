#! /usr/bin/env python3
"""Fetch the ``eksport/moter`` overview for one sesjon-year.

Writes ``original/meetings/{sesjonid}.json`` containing the full
``moter_liste`` from the API. Each list entry carries:

  - ``id``           — moteid (used for video lookup and proceedings URL)
  - ``mote_dato_tid``— scheduled meeting start in ``/Date(epoch_ms+TZ)/`` form
  - ``referat_id``   — proceedings publikasjon id (e.g. ``refs-202526-10-14``)
                       or ``null`` for non-substantive entries (PACE travels)
  - ``mote_ting``    — 0 = none, 1 = ordinary plenary, 2 = constitutional
  - ``mote_rekkefolge`` — ordinal within the sesjon-year
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


def _fetch_bytes(url: str, *, retry_count: int, retry_delay_max: float,
                 accept: str = "application/json") -> bytes:
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            req = Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": accept,
            })
            with urlopen(req, timeout=60) as resp:
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
                logger.warning(f"{type(e).__name__} on {url}: {e}, retry {attempt}/{retry_count}")
                time.sleep(min(delay, retry_delay_max))
                delay = min(delay * 2, retry_delay_max)
                last_exc = e
                continue
            raise
    raise RuntimeError(f"Exhausted {retry_count} attempts for {url}: {last_exc}")


def _strip_bom(b: bytes) -> bytes:
    return b[3:] if b.startswith(b"\xef\xbb\xbf") else b


def fetch_meetings(config: Config, sesjonid: str, *, force: bool = False,
                   retry_count: int = 10, retry_delay_max: float = 10.0) -> Path:
    target = config.dir("meetings", create=True) / f"{sesjonid}.json"
    if target.exists() and not force:
        logger.info(f"Meetings cache hit: {target.name} (use --force to refetch)")
        return target
    url = f"{API_ROOT}/moter?sesjonid={sesjonid}&format=json"
    logger.info(f"GET {url}")
    body = _strip_bom(_fetch_bytes(url, retry_count=retry_count,
                                   retry_delay_max=retry_delay_max))
    doc = json.loads(body)
    moter_liste = doc.get("moter_liste") or []
    # Drop entries with id < 0 (PACE travels etc. that the API mixes in).
    cleaned = [m for m in moter_liste if (m.get("id") or 0) > 0]
    doc["moter_liste"] = cleaned
    target.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"  wrote {target.name} ({len(cleaned)} meeting(s))")
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--sesjon", required=True, help="sesjon-id e.g. 2025-2026")
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    fetch_meetings(config, args.sesjon, force=args.force,
                   retry_count=args.retry_count, retry_delay_max=args.retry_delay_max)


if __name__ == "__main__":
    main()
