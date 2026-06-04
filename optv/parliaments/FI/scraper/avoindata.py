#! /usr/bin/env python3
"""Tiny client for the Eduskunta avoindata REST API.

``https://avoindata.eduskunta.fi/api/v1/tables/{table}/rows`` returns rows as
positional arrays plus a ``columnNames`` list. This module exposes them as
dicts and adds the two access patterns the FI scraper needs:

- :func:`filter_rows` — server-side ``columnName`` / ``columnValue`` filter.
- :func:`fetch_ptk_xml` — pull the verbatim PTK plenary-minutes XML for a
  session from the ``VaskiData`` document store (keyed by ``Eduskuntatunnus``,
  e.g. ``"PTK 58/2026 vp"``), picking the richest candidate when the store
  holds several versions of the same minutes.

All proceedings text the pipeline needs lives in these PTK documents — the
``SaliDBPuheenvuoro`` speech-turn table is metadata-only (its ``XmlData``
column is empty even for current rows), so we do not use it.
"""

from __future__ import annotations

import json
import logging
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

API_ROOT = "https://avoindata.eduskunta.fi/api/v1/tables"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")


def _get_json(url: str, *, timeout: float = 60.0,
              retry_count: int = 5, retry_delay_max: float = 10.0) -> dict:
    req = Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
    delay = 1.0
    for attempt in range(1, retry_count + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt >= retry_count:
                raise
            logger.warning(f"avoindata retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, retry_delay_max)
    raise RuntimeError("unreachable")


def _rows_to_dicts(payload: dict) -> list[dict]:
    cols = payload.get("columnNames") or []
    return [dict(zip(cols, row)) for row in (payload.get("rowData") or [])]


def filter_rows(table: str, column: str, value: str, *,
                per_page: int = 100, max_pages: int = 50, **kw) -> list[dict]:
    """Return all rows of ``table`` where ``column == value`` (server-side filter)."""
    out: list[dict] = []
    for page in range(max_pages):
        qs = urlencode({"columnName": column, "columnValue": value,
                        "perPage": per_page, "page": page})
        payload = _get_json(f"{API_ROOT}/{table}/rows?{qs}", **kw)
        rows = _rows_to_dicts(payload)
        out.extend(rows)
        if not payload.get("hasMore"):
            break
    return out


def _count_speeches(xml: str) -> int:
    """Number of ``PuheenvuoroToimenpide`` (speech-turn) elements in a PTK doc."""
    return len(re.findall(r"<(?:\w+:)?PuheenvuoroToimenpide[ >]", xml or ""))


def fetch_ptk_xml(number: int, year: int, **kw) -> str | None:
    """Return the verbatim PTK XML for session ``{number}/{year}``.

    ``VaskiData`` can hold several rows under the same ``Eduskuntatunnus``
    (draft / final / language variants). We keep the one with the most
    speech-turn elements — the complete final Finnish minutes — and ignore the
    per-agenda-item chunks (those carry the item number in their tunnus and so
    don't match the exact ``"PTK N/YYYY vp"`` filter).
    """
    tunnus = f"PTK {number}/{year} vp"
    rows = filter_rows("VaskiData", "Eduskuntatunnus", tunnus, per_page=20, max_pages=3, **kw)
    candidates = [r for r in rows
                  if (r.get("Eduskuntatunnus") or "").strip() == tunnus
                  and (r.get("XmlData") or "").strip()]
    if not candidates:
        return None
    best = max(candidates, key=lambda r: _count_speeches(r["XmlData"]))
    logger.info(f"PTK {tunnus}: {len(candidates)} candidate(s), "
                f"kept {_count_speeches(best['XmlData'])} speech-turns "
                f"({len(best['XmlData'])} bytes)")
    return best["XmlData"]
