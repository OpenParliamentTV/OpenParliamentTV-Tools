"""Build ``metadata/entities.json`` for the NL entity linking from Wikidata.

Persons come from two queries:

1. Everyone who has held the position "member of the House of Representatives" (``P39 wd:Q18887908``), as ``memberOfParliament``. They are written oldest term first, so that a name two members share resolves to the more recent one.
2. Everyone who has held a Dutch cabinet office since ``OFFICE_SINCE``: a position of the country Netherlands that is a kind of minister or state secretary. Ministers and state secretaries need not be members; those who never were are written as ``person``.

A person's label is the Dutch one, else the language-independent one, else the English one. The other variants and the Dutch aliases of more than one word become ``labelAlternative``; a member the report names differently from Wikidata is linked once Wikidata lists that name as an alias.

Factions are a fixed list under the names the report uses.

Run: ``python -m optv.parliaments.NL.scraper.build_entity_dump <data_dir>``.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from optv.shared.cli import load_config, setup_logging
from optv.shared.entity_dump_bootstrap import bind, sparql_get

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

# Cabinet members are taken from this date on: the cabinets whose members can
# still appear in the periods this parliament is set up for.
OFFICE_SINCE = "2017-01-01"

_PERSON_FIELDS = """
  OPTIONAL { ?person rdfs:label ?nl  FILTER(LANG(?nl) = "nl") }
  OPTIONAL { ?person rdfs:label ?mul FILTER(LANG(?mul) = "mul") }
  OPTIONAL { ?person rdfs:label ?en  FILTER(LANG(?en) = "en") }
  OPTIONAL { ?person skos:altLabel ?alias FILTER(LANG(?alias) IN ("nl", "mul")) }
"""
_PERSON_SELECT = """SELECT ?person (SAMPLE(?nl) AS ?label_nl) (SAMPLE(?mul) AS ?label_mul) (SAMPLE(?en) AS ?label_en)
       (GROUP_CONCAT(DISTINCT ?alias; separator="|") AS ?aliases) (MAX(?start) AS ?latest)"""

# p:/ps: and not wdt:, so that a membership statement of non-preferred rank
# (a member who later joined the cabinet) still counts.
SPARQL_MEMBERS = _PERSON_SELECT + """ WHERE {
  ?person p:P39 ?statement .
  ?statement ps:P39 wd:Q18887908 .
  OPTIONAL { ?statement pq:P580 ?start }
""" + _PERSON_FIELDS + "} GROUP BY ?person"

# Q83307 minister; Q736559 state secretary and Q1847103, the Dutch office of that
# name, since the offices are filed under either; Q55 the Netherlands.
SPARQL_CABINET = _PERSON_SELECT + """ WHERE {
  ?person p:P39 ?statement .
  ?statement ps:P39 ?office ; pq:P580 ?start .
  ?office wdt:P17 wd:Q55 ; wdt:P279* ?kind .
  VALUES ?kind { wd:Q83307 wd:Q736559 wd:Q1847103 }
  FILTER(?start >= "%(since)sT00:00:00Z"^^xsd:dateTime)
""" + _PERSON_FIELDS + "} GROUP BY ?person"

# ``label`` is the name the report gives a faction; ``aliases`` add the full name.
NL_PARTIES: list[dict] = [
    {"id": "Q239333", "label": "VVD", "aliases": ["Volkspartij voor Vrijheid en Democratie"]},
    {"id": "Q747910", "label": "D66", "aliases": ["Democraten 66"]},
    {"id": "Q332739", "label": "PVV", "aliases": ["Partij voor de Vrijheid"]},
    {"id": "Q273749", "label": "CDA", "aliases": ["Christen-Democratisch Appèl"]},
    {"id": "Q138795640", "label": "PRO", "aliases": ["Progressief Nederland"]},
    {"id": "Q110853869", "label": "GroenLinks-PvdA", "aliases": ["GL-PvdA"]},
    {"id": "Q104244010", "label": "JA21", "aliases": []},
    {"id": "Q28163962", "label": "FVD", "aliases": ["Forum voor Democratie"]},
    {"id": "Q101083924", "label": "BBB", "aliases": ["BoerBurgerBeweging"]},
    {"id": "Q849580", "label": "SP", "aliases": ["Socialistische Partij"]},
    {"id": "Q663535", "label": "PvdD", "aliases": ["Partij voor de Dieren"]},
    {"id": "Q239539", "label": "ChristenUnie", "aliases": ["CU"]},
    {"id": "Q60172", "label": "SGP", "aliases": ["Staatkundig Gereformeerde Partij"]},
    {"id": "Q28147283", "label": "DENK", "aliases": []},
    {"id": "Q59574", "label": "50PLUS", "aliases": []},
    {"id": "Q105412880", "label": "Volt", "aliases": ["Volt Nederland"]},
    {"id": "Q121705847", "label": "NSC", "aliases": ["Nieuw Sociaal Contract"]},
    {"id": "Q137827474", "label": "Groep Markuszower", "aliases": []},
]


def parse_persons(payload: dict) -> dict[str, dict]:
    """``{qid: {id, label, aliases, latest}}`` from a person query's response; an item without a usable label is left out."""
    persons: dict[str, dict] = {}
    for b in (payload.get("results") or {}).get("bindings") or []:
        qid = (bind(b, "person") or "").rsplit("/", 1)[-1]
        names = [(bind(b, key) or "").strip() for key in ("label_nl", "label_mul", "label_en")]
        label = next((n for n in names if n), "")
        if not qid or not label:
            continue
        # An alias of one word is a bare surname or a first name: no key for a person.
        aliases = [a.strip() for a in (bind(b, "aliases") or "").split("|") if len(a.split()) > 1]
        persons[qid] = {
            "id": qid,
            "label": label,
            "aliases": sorted({n for n in [*names, *aliases] if n} - {label}),
            "latest": bind(b, "latest") or "",
        }
    return persons


def build_entities(members: dict[str, dict], cabinet: dict[str, dict]) -> dict:
    """The entity document from the two person sets and the party list."""
    data: list[dict] = []
    for person in sorted(members.values(), key=lambda p: (p["latest"], p["id"])):
        data.append({"id": person["id"], "label": person["label"], "labelAlternative": person["aliases"],
                     "type": "person", "subType": "memberOfParliament"})
    others = [p for qid, p in cabinet.items() if qid not in members]
    for person in sorted(others, key=lambda p: (p["latest"], p["id"])):
        data.append({"id": person["id"], "label": person["label"], "labelAlternative": person["aliases"],
                     "type": "person", "subType": "person"})
    for party in NL_PARTIES:
        data.append({"id": party["id"], "label": party["label"], "labelAlternative": party["aliases"],
                     "type": "organisation", "subType": "faction"})
    logger.info(f"Built {len(members)} members, {len(others)} cabinet members who never sat in the House, "
                f"{len(NL_PARTIES)} factions")
    return {
        "meta": {"source": "Wikidata SPARQL (P39 Q18887908 members; Dutch minister and state secretary "
                           f"offices since {OFFICE_SINCE}) and the party list of this module"},
        "data": data,
    }


def write_entity_dump(config) -> Path:
    logger.info("Querying Wikidata for members of the House and for cabinet members…")
    members = parse_persons(sparql_get(SPARQL_MEMBERS))
    cabinet = parse_persons(sparql_get(SPARQL_CABINET % {"since": OFFICE_SINCE}))
    doc = build_entities(members, cabinet)
    target = config.dir("nel_data", create=True) / "entities.json"
    target.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Wrote {target} ({len(doc['data'])} entities)")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    write_entity_dump(load_config(PARLIAMENT_ID, args.data_dir))


if __name__ == "__main__":
    main()
