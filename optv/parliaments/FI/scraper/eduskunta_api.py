#! /usr/bin/env python3
"""Tiny client for the Eduskunta open data API (``https://api.eduskunta.fi``).

The API is documented with OpenAPI (``https://api.eduskunta.fi/openapi.json``),
needs no key and allows 180 GET / 90 POST requests per minute per IP. The FI
scraper uses three calls:

- :func:`list_sittings` — every plenary sitting that has speeches, with its
  speech count, from ``POST /api/v1/aggregations/unique-by``. That endpoint
  ignores the ``expression`` filter, so it aggregates all years and callers
  filter client-side.
- :func:`fetch_speeches` — the speech records of one sitting from
  ``POST /api/v1/search`` (category ``puheenvuoro``): speech id, agenda item,
  speaker (person number, names, office, party, group), UTC start/end, text and
  the record's status (``tila``).
- :func:`fetch_mp_reference` — the MP register (person number + names) from
  ``GET /api/v1/reference-data/kansanedustajat``.

Requests are spaced at least :data:`MIN_INTERVAL_S` apart. Large search results
come back as a 302 redirect to a presigned download URL, which ``urllib``
follows as a plain GET.
"""

from __future__ import annotations

import json
import logging
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

API_ROOT = "https://api.eduskunta.fi/api/v1"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

# Minimum spacing between two requests (the POST limit is 90 per minute).
MIN_INTERVAL_S = 1.0
# The search endpoint returns at most this many records per request; a sitting
# has a few hundred speeches at most.
MAX_RESULTS = 1000

_last_request = 0.0


def _pace() -> None:
    global _last_request
    wait = _last_request + MIN_INTERVAL_S - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_request = time.monotonic()


def _request_json(path: str, body: dict | None = None, *, timeout: float = 120.0,
                  retry_count: int = 5, retry_delay_max: float = 10.0):
    """GET (``body is None``) or POST ``body`` as JSON to ``API_ROOT + path``.

    Retries on network errors, 429 and 5xx with exponential backoff; any other
    HTTP error is raised at once.
    """
    url = API_ROOT + path
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    delay = 1.0
    for attempt in range(1, retry_count + 1):
        _pace()
        req = Request(url, data=data, headers=headers,
                      method="POST" if data is not None else "GET")
        try:
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            if e.code != 429 and e.code < 500:
                raise
            if attempt >= retry_count:
                raise
            retry_after = e.headers.get("Retry-After") if e.headers else None
            if retry_after and retry_after.isdigit():
                delay = max(delay, float(retry_after))
            logger.warning(f"api.eduskunta.fi retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
        except (URLError, TimeoutError) as e:
            if attempt >= retry_count:
                raise
            logger.warning(f"api.eduskunta.fi retry {attempt}/{retry_count} after {delay:.1f}s: {e}")
        time.sleep(delay)
        delay = min(delay * 2, retry_delay_max)
    raise RuntimeError("unreachable")


def parse_sitting_aggregation(payload: dict) -> dict[tuple[int, int], int]:
    """``{(year, number): speech_count}`` from a unique-by aggregation response."""
    out: dict[tuple[int, int], int] = {}
    for r in payload.get("results") or []:
        key = r.get("key") or {}
        try:
            year = int(key.get("valtiopaivavuosi"))
            number = int(key.get("taysistuntonumero"))
        except (TypeError, ValueError):
            continue
        out[(year, number)] = int(r.get("docCount") or 0)
    return out


def list_sittings(**kw) -> dict[tuple[int, int], int]:
    """Every plenary sitting with speeches, as ``{(year, number): speech_count}``."""
    payload = _request_json("/aggregations/unique-by", {
        "category": "puheenvuoro",
        "agg": {"unique": {"terms": ["valtiopaivavuosi", "taysistuntonumero"]}},
    }, **kw)
    return parse_sitting_aggregation(payload)


def _speech_sort_key(rec: dict) -> tuple:
    """Order by start time, then by the numeric parts of the speech id."""
    parts = []
    for p in (rec.get("id") or "").replace("PUH ", "").split("/"):
        parts.append(int(p) if p.isdigit() else 0)
    return (rec.get("aloitushetki") or "", parts)


def fetch_speeches(year: int, number: int, **kw) -> list[dict]:
    """Speech records (``puheenvuoro`` objects) of sitting ``number/year``, in order."""
    payload = _request_json("/search", {
        "category": "puheenvuoro",
        "maxResults": MAX_RESULTS,
        "expression": {"and": [
            {"property": "valtiopaivavuosi", "match": str(year)},
            {"property": "taysistuntonumero", "match": str(number)},
        ]},
        "sort": [{"property": "aloitushetki", "ascending": True}],
    }, **kw)
    meta = payload.get("searchMetadata") or {}
    total = meta.get("totalResultCount")
    records = [r["puheenvuoro"] for r in payload.get("results") or []
               if isinstance(r.get("puheenvuoro"), dict)]
    # The filter is a text match; keep only exact hits.
    records = [r for r in records
               if str(r.get("valtiopaivavuosi")) == str(year)
               and str(r.get("taysistuntonumero")) == str(number)]
    if total is not None and total > len(payload.get("results") or []):
        logger.warning(f"{number}/{year}: {total} speeches, only "
                       f"{len(payload.get('results') or [])} returned")
    return sorted(records, key=_speech_sort_key)


def fetch_mp_reference(**kw) -> list[dict]:
    """The MP register: ``[{tunnus (person number), nimi, sukunimi, aktiivinen, …}]``."""
    payload = _request_json("/reference-data/kansanedustajat", **kw)
    return payload if isinstance(payload, list) else []
