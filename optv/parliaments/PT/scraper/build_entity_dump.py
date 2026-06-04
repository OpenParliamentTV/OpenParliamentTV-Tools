#! /usr/bin/env python3
"""Build ``metadata/entities.json`` (NEL) for PT from Wikidata.

Unlike FR/DE there is no parliament-side roster open-data to join — the
av.parlamento.pt JSON names each speaker (and party) but carries no Wikidata or
BID identifier. So the dump is Wikidata-only:

1. **Persons** — every member of the Assembly of the Republic
   (``P39 wd:Q19953703``), keyed by normalised name, ``subType:
   memberOfParliament``.
2. **Factions** — the parliamentary parties those members belong to
   (``P102``), keyed by short name (``P1813``), ``subType: faction``.

``optv.shared.nel`` reads this file and matches the per-speech ``people[].label``
/ ``faction.label`` against it. Name matching is imperfect for Portuguese
(hyphenated surnames, all-caps source names), so coverage is partial — the same
best-effort contract as FR/NO; a miss is a warning, not an error. The shape
mirrors the platform entity-dump (``id``/``label``/``labelAlternative``/``type``/
``subType``) so a future live pull is a drop-in. Run::

    python -m optv.parliaments.PT.scraper.build_entity_dump <data_dir>
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import unicodedata
from collections import OrderedDict
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.PT.scraper"

from optv.parliaments.PT.common import Config
from optv.parliaments.PT.scraper.common import USER_AGENT

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# P39 wd:Q19953703 = "member of the Assembly of the Republic" (Portugal). p:/ps:
# so former memberships are followed too — once an MP, always a former MP.
SPARQL_PT_MEMBERS = """
SELECT DISTINCT ?person ?label_pt ?label_en WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q19953703 .
  OPTIONAL { ?person rdfs:label ?label_pt FILTER(LANG(?label_pt)="pt") }
  OPTIONAL { ?person rdfs:label ?label_en FILTER(LANG(?label_en)="en") }
}
"""

# Parties (P102) of Assembly members, with their short name (P1813). Aggregated
# by descending member count so the major current parties surface first.
SPARQL_PT_PARTIES = """
SELECT ?party ?label_pt ?label_en ?abbrev (COUNT(DISTINCT ?person) AS ?n) WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q19953703 .
  ?person wdt:P102 ?party .
  OPTIONAL { ?party rdfs:label ?label_pt FILTER(LANG(?label_pt)="pt") }
  OPTIONAL { ?party rdfs:label ?label_en FILTER(LANG(?label_en)="en") }
  OPTIONAL { ?party wdt:P1813 ?abbrev }
}
GROUP BY ?party ?label_pt ?label_en ?abbrev
ORDER BY DESC(?n)
"""

# Verified current-party QIDs (pinned because Wikidata's P1813 short names are
# uneven). Anything not pinned still resolves dynamically from SPARQL.
PARTY_QIDS_OVERRIDE: dict[str, str] = {
    "BE": "Q884840",        # Bloco de Esquerda
    "IL": "Q46122950",      # Iniciativa Liberal
    "PCP": "Q769829",       # Partido Comunista Português
}


def _clean(s: str) -> str:
    """Mirror ``optv.shared.nel.cleanup`` closely enough for join keys."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().replace("-", " ").split())


def _sparql_get(query: str, *, timeout: float = 120.0) -> dict:
    url = SPARQL_ENDPOINT + "?" + urlencode({"query": query})
    req = Request(url, headers={"Accept": "application/sparql-results+json",
                                "User-Agent": USER_AGENT})
    delay = 5.0
    for attempt in range(1, 7):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt >= 6:
                raise
            logger.warning(f"SPARQL retry {attempt} after {delay:.0f}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 70)
    raise RuntimeError("unreachable")


def _bind(b: dict, key: str) -> str | None:
    v = b.get(key)
    return v.get("value") if isinstance(v, dict) else None


def fetch_members() -> "OrderedDict[str, dict]":
    """Return ``{clean_name: {id, label, aliases}}`` for Assembly members."""
    logger.info("Querying Wikidata for members of the Assembly of the Republic…")
    try:
        payload = _sparql_get(SPARQL_PT_MEMBERS)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Wikidata members query failed ({e}); persons will have no QID")
        return OrderedDict()
    persons: "OrderedDict[str, dict]" = OrderedDict()
    for b in (payload.get("results") or {}).get("bindings") or []:
        iri = _bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        label = _bind(b, "label_pt") or _bind(b, "label_en") or ""
        if not label:
            continue
        key = _clean(label)
        if not key:
            continue
        entry = persons.get(key)
        if entry is None:
            aliases = []
            en = _bind(b, "label_en")
            if en and en != label:
                aliases.append(en)
            persons[key] = {"id": qid, "label": label, "aliases": aliases}
    logger.info(f"Wikidata: {len(persons)} distinct Assembly members")
    return persons


def fetch_parties() -> list[dict]:
    """Return ``[{qid, label, label_en, abbrev, n}]`` for Assembly parties."""
    logger.info("Querying Wikidata for Assembly member parties…")
    try:
        payload = _sparql_get(SPARQL_PT_PARTIES)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Wikidata parties query failed ({e}); factions will have no QID")
        return []
    out: list[dict] = []
    for b in (payload.get("results") or {}).get("bindings") or []:
        iri = _bind(b, "party") or ""
        if not iri:
            continue
        out.append({
            "qid": iri.rsplit("/", 1)[-1],
            "label": _bind(b, "label_pt") or _bind(b, "label_en") or "",
            "label_en": _bind(b, "label_en") or "",
            "abbrev": _bind(b, "abbrev") or "",
            "n": int(_bind(b, "n") or 0),
        })
    logger.info(f"Wikidata: {len(out)} parties")
    return out


def build() -> dict:
    persons = fetch_members()
    parties = fetch_parties()

    data: list[dict] = []
    for entry in persons.values():
        data.append({
            "id": entry["id"],
            "label": entry["label"],
            "labelAlternative": entry["aliases"],
            "type": "person",
            "subType": "memberOfParliament",
        })

    # One faction entry per distinct abbreviation (the av affiliation `initials`
    # are the per-speech faction labels NEL matches against). Pinned overrides
    # win; otherwise the abbrev's most-populous party QID is used.
    factions: "OrderedDict[str, dict]" = OrderedDict()
    for p in parties:
        ab = (p["abbrev"] or "").strip()
        if not ab:
            continue
        if ab not in factions:
            qid = PARTY_QIDS_OVERRIDE.get(ab) or p["qid"]
            factions[ab] = {
                "id": qid,
                "label": ab,
                "labelAlternative": [x for x in (p["label"], p["label_en"]) if x],
                "type": "organisation",
                "subType": "faction",
            }
    # Ensure pinned current parties are present even if SPARQL lacked their abbrev.
    for ab, qid in PARTY_QIDS_OVERRIDE.items():
        factions.setdefault(ab, {
            "id": qid, "label": ab, "labelAlternative": [],
            "type": "organisation", "subType": "faction",
        })
    data.extend(factions.values())

    with_qid = sum(1 for e in data if e["type"] == "person" and e["id"])
    logger.info(f"Built {len(persons)} persons ({with_qid} with QID), "
                f"{len(factions)} factions")
    return {
        "meta": {"source": "Wikidata SPARQL (P39 Q19953703 persons; P102 parties)"},
        "data": data,
    }


def write_entity_dump(config: Config, leg: int = 17) -> Path:
    target_dir = config.dir("nel_data", create=True)
    entities = build()
    entities_path = target_dir / "entities.json"
    entities_path.write_text(json.dumps(entities, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {entities_path} ({len(entities['data'])} entities)")
    return entities_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    write_entity_dump(config, args.period)


if __name__ == "__main__":
    main()
