#! /usr/bin/env python3
"""Shared machinery for the *temporary* pre-platform entity-dump builders.

**Scope:** this is throwaway scaffolding. Each parliament's
``scraper/build_entity_dump.py`` produces a local ``metadata/entities.json`` so
the NEL stage has something to link against *before* that parliament's platform
instance exists. Once the platform serves entities via the manifest
``entity_dump_url`` (as DE already does), these builders are retired. So this
module only de-duplicates the identical Wikidata-SPARQL plumbing the builders
shared — it is deliberately small and not wired into the pipeline.

Per-parliament *data* (the P39 membership QID, the party list) stays in each
builder; only the transport/emit helpers live here.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")


def sparql_get(query: str, *, timeout: float = 120.0) -> dict:
    """Run a SPARQL query against Wikidata; retry with backoff on transport errors."""
    url = SPARQL_ENDPOINT + "?" + urlencode({"query": query})
    req = Request(url, headers={
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT,
    })
    delay = 2.0
    for attempt in range(1, 5):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt >= 4:
                raise
            logger.warning(f"SPARQL retry {attempt} after {delay:.1f}s: {e}")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def bind(binding: dict, key: str) -> str | None:
    """Pull a scalar value out of a SPARQL JSON result binding."""
    v = binding.get(key)
    return v.get("value") if isinstance(v, dict) else None


def write_entity_dump(metadata_dir: Path, payload: dict) -> Path:
    """Write ``metadata/entities.json`` deterministically; return its path."""
    metadata_dir.mkdir(parents=True, exist_ok=True)
    out = metadata_dir / "entities.json"
    with open(out, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return out
