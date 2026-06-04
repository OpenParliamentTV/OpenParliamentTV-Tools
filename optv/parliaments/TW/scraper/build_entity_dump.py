#! /usr/bin/env python3
"""Build ``metadata/entities.json`` for the TW NEL stage.

Two sources, joined on Chinese label:

1. **Wikidata SPARQL** — everyone who has held the position
   ``P39 wd:Q6310593`` ("Member of the Legislative Yuan"). Provides the
   Wikidata QID plus all label variants (zh, zh-TW, en, …).
2. **ly.govapi.tw/v2/legislators** — current term roster. Provides the
   canonical Chinese name (``委員姓名``) and 黨團 (faction) for any LY
   member whom Wikidata doesn't (yet) cover; these are written WITHOUT a
   QID so downstream NER can at least surface the speaker entity even
   if it isn't linked.

Faction entries are added from a small hand-curated mapping of major
Taiwanese parties (DPP/KMT/TPP and the three smaller post-2024 caucuses);
the LY API's ``黨團`` field uses the Chinese names verbatim.

Run:

    python -m optv.parliaments.TW.scraper.build_entity_dump <data_dir>
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import OrderedDict
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                          # TW/
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))     # repo root
    __package__ = "optv.parliaments.TW.scraper"

from optv.parliaments.TW.common import Config
from .ly_api import LYApiClient

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

# Q6310593 = "Member of the Legislative Yuan" (national legislative body of
# the Republic of China). All people who ever held that position.
SPARQL_LY_MEMBERS = """
SELECT DISTINCT ?person ?label_en ?label_zh ?label_zh_tw ?label_zh_hant WHERE {
  ?person wdt:P39 wd:Q6310593 .
  OPTIONAL { ?person rdfs:label ?label_en      FILTER(LANG(?label_en)="en") }
  OPTIONAL { ?person rdfs:label ?label_zh      FILTER(LANG(?label_zh)="zh") }
  OPTIONAL { ?person rdfs:label ?label_zh_tw   FILTER(LANG(?label_zh_tw)="zh-tw") }
  OPTIONAL { ?person rdfs:label ?label_zh_hant FILTER(LANG(?label_zh_hant)="zh-hant") }
}
"""

# Taiwanese political parties with current LY caucuses + a few historical ones.
# `label` should match the Chinese form returned by ly.govapi.tw's ``黨團`` /
# ``黨籍`` fields exactly; `aliases` carries alternative spellings.
TW_PARTIES: list[dict] = [
    {"id": "Q57251",      "label": "中國國民黨",   "aliases": ["國民黨", "KMT", "Kuomintang"]},
    {"id": "Q41700",      "label": "民主進步黨",   "aliases": ["民進黨", "DPP", "Democratic Progressive Party"]},
    {"id": "Q63124871",   "label": "台灣民眾黨",   "aliases": ["民眾黨", "TPP", "Taiwan People's Party"]},
    {"id": "Q19834712",   "label": "時代力量",     "aliases": ["NPP", "New Power Party"]},
    {"id": "Q1163646",    "label": "親民黨",       "aliases": ["PFP", "People First Party"]},
    {"id": "Q713750",     "label": "新黨",         "aliases": ["NP", "New Party"]},
    {"id": "Q713812",     "label": "台灣團結聯盟", "aliases": ["台聯", "TSU"]},
]


def _sparql_get(query: str, *, timeout: float = 90.0) -> dict:
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
            logger.warning("SPARQL retry %d after %.1fs: %s", attempt, delay, e)
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def _bind(binding: dict, key: str) -> str | None:
    v = binding.get(key)
    return v.get("value") if isinstance(v, dict) else None


def fetch_wikidata_ly_members() -> list[dict]:
    """Query Wikidata for all known holders of P39=Q6310593."""
    logger.info("Querying Wikidata for LY members…")
    payload = _sparql_get(SPARQL_LY_MEMBERS)
    bindings = (payload.get("results") or {}).get("bindings") or []
    members: dict[str, dict] = OrderedDict()
    for b in bindings:
        iri = _bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        label_zh = _bind(b, "label_zh") or _bind(b, "label_zh_tw") or _bind(b, "label_zh_hant")
        label_en = _bind(b, "label_en")
        primary = label_zh or label_en
        if not primary:
            continue
        if qid not in members:
            alts: list[str] = []
            for k in ("label_en", "label_zh", "label_zh_tw", "label_zh_hant"):
                v = _bind(b, k)
                if v and v != primary and v not in alts:
                    alts.append(v)
            members[qid] = {"qid": qid, "label": primary, "aliases": alts}
        else:
            ent = members[qid]
            for k in ("label_en", "label_zh", "label_zh_tw", "label_zh_hant"):
                v = _bind(b, k)
                if v and v != ent["label"] and v not in ent["aliases"]:
                    ent["aliases"].append(v)
    logger.info(f"Wikidata returned {len(members)} distinct LY members.")
    return list(members.values())


def fetch_ly_api_legislators(term: int) -> list[dict]:
    """Pull the current-term roster from ly.govapi.tw for fallback names."""
    client = LYApiClient(min_interval=0.4)
    rows: list[dict] = []
    for row in client.iter_legislators(term):
        name_zh = (row.get("委員姓名") or "").strip()
        if not name_zh:
            continue
        name_en = (row.get("委員英文姓名") or "").strip()
        faction = (row.get("黨團") or row.get("黨籍") or "").strip()
        rows.append({
            "label": name_zh,
            "aliases": [name_en] if name_en else [],
            "faction": faction,
        })
    logger.info(f"LY API returned {len(rows)} legislators for term {term}.")
    return rows


def build_entities(term: int) -> dict:
    members = fetch_wikidata_ly_members()
    api_legislators = fetch_ly_api_legislators(term)

    # Index Wikidata members by their cleaned Chinese label for the merge step.
    from optv.shared.nel import cleanup
    by_clean: dict[str, dict] = {}
    for m in members:
        for candidate in (m["label"], *m["aliases"]):
            key = cleanup(candidate)
            if key and key not in by_clean:
                by_clean[key] = m

    data: list[dict] = []
    seen_qids: set[str] = set()

    for m in members:
        if m["qid"] in seen_qids:
            continue
        seen_qids.add(m["qid"])
        data.append({
            "id": m["qid"],
            "label": m["label"],
            "labelAlternative": m["aliases"],
            "type": "person",
            "subType": "memberOfParliament",
        })

    # Add LY-API legislators that Wikidata doesn't cover (no QID).
    added = 0
    for leg in api_legislators:
        key = cleanup(leg["label"])
        if key in by_clean:
            continue
        data.append({
            "id": "",
            "label": leg["label"],
            "labelAlternative": leg["aliases"],
            "type": "person",
            "subType": "memberOfParliament",
        })
        added += 1
    logger.info(f"Added {added} LY-API-only legislators (no Wikidata QID).")

    # Factions
    for party in TW_PARTIES:
        data.append({
            "id": party["id"],
            "label": party["label"],
            "labelAlternative": party["aliases"],
            "type": "organisation",
            "subType": "faction",
        })

    return {
        "meta": {"source": "Wikidata + ly.govapi.tw", "term": term},
        "data": data,
    }


def write_entity_dump(config: Config, term: int) -> Path:
    target_dir = config.dir("nel_data", create=True)
    target = target_dir / "entities.json"
    doc = build_entities(term)
    target.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {target} ({len(doc['data'])} entities)")
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--term", type=int, default=11,
                        help="Legislative term to pull legislators for (default 11)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = Config(args.data_dir)
    write_entity_dump(config, args.term)


if __name__ == "__main__":
    main()
