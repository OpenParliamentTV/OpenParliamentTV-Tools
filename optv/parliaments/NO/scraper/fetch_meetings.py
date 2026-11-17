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
import datetime
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


def sesjon_end(sesjonid: str) -> datetime.date | None:
    """First day after a sesjon-year (``YYYY-YYYY``): 1 October of the end year.

    A sesjon runs from October to the end of September; the next one opens on
    1 October of the end year.
    """
    try:
        return datetime.date(int(sesjonid.split("-")[1]), 10, 1)
    except (IndexError, ValueError):
        return None


def sesjon_ended(sesjonid: str, today: datetime.date | None = None) -> bool:
    end = sesjon_end(sesjonid)
    return end is not None and (today or datetime.date.today()) >= end


def response_date(doc: dict) -> datetime.date | None:
    """Date of the API response that produced a cached overview
    (``respons_dato_tid``, ``/Date(epoch_ms+TZ)/``)."""
    m = re.match(r"/Date\((-?\d+)", doc.get("respons_dato_tid") or "")
    if not m:
        return None
    return datetime.datetime.fromtimestamp(int(m.group(1)) / 1000,
                                           tz=datetime.timezone.utc).date()


def overview_is_final(doc: dict, sesjonid: str) -> bool:
    """True when the cached overview was fetched after its sesjon ended.

    Until then it keeps gaining meetings and Referat ids, so it is refetched.
    Decided from the response date stored in the file, not the file's mtime,
    so a fresh checkout of the data repository behaves the same.
    """
    end, fetched = sesjon_end(sesjonid), response_date(doc)
    return end is not None and fetched is not None and fetched >= end


def _read_cached(target: Path) -> dict | None:
    if not target.exists():
        return None
    try:
        return json.loads(target.read_text())
    except json.JSONDecodeError:
        return None


def fetch_meetings(config: Config, sesjonid: str, *, force: bool = False,
                   retry_count: int = 10, retry_delay_max: float = 10.0) -> Path:
    target = config.dir("meetings", create=True) / f"{sesjonid}.json"
    cached = _read_cached(target)
    if cached is not None and not force and overview_is_final(cached, sesjonid):
        logger.info(f"Meetings cache hit: {target.name} (use --force to refetch)")
        return target
    url = f"{API_ROOT}/moter?sesjonid={sesjonid}&format=json"
    logger.info(f"GET {url}")
    body = strip_bom(fetch_bytes(url, accept="application/json",
                                 retry_count=retry_count,
                                 retry_delay_max=retry_delay_max))
    doc = json.loads(body)
    moter_liste = doc.get("moter_liste") or []
    # Drop entries with id < 0 (PACE travels etc. that the API mixes in).
    cleaned = [m for m in moter_liste if (m.get("id") or 0) > 0]
    doc["moter_liste"] = cleaned
    # The response timestamp changes on every call: rewrite only when the
    # meeting list changed, or to record the first fetch after the sesjon
    # ended (which makes the overview final).
    if (cached is not None and cached.get("moter_liste") == cleaned
            and overview_is_final(cached, sesjonid) == overview_is_final(doc, sesjonid)):
        logger.info(f"  {target.name} unchanged ({len(cleaned)} meeting(s))")
        return target
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
