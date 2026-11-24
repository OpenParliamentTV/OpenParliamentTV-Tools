"""Build ``metadata/entities.json`` for the IT entity linking from Wikidata.

Persons come from three queries:

1. Everyone who has held the position "deputato della Repubblica Italiana" (``P39 wd:Q18558478``), as ``memberOfParliament``, with the Chamber's person id (``P1341``) as ``additionalInformation.originPersonID``. The WebTV page and the record name every speaker by that id, so a deputy is linked by id and not by name. They are written oldest term first, so that a name two deputies share resolves to the more recent one.
2. Everyone who has been a senator (``P39 wd:Q13653224``) since ``OTHERS_SINCE``, as ``person``: members of the government who sit in the Senate speak in the Chamber too.
3. Everyone who has held an office of minister of Italy since ``OTHERS_SINCE``, as ``person``, for ministers who sit in neither house.

The people of the second and third query have no Chamber id on Wikidata and are linked by name. A person's label is the Italian one, else the language-independent one, else the English one; the other variants and the Italian aliases of more than one word become ``labelAlternative``.

Factions are a fixed list of the parliamentary groups under the names the WebTV page writes, with the abbreviations of the record as alternatives.

Run: ``python -m optv.parliaments.IT.scraper.build_entity_dump <data_dir>``.
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

# Senators and ministers are taken from this date on: the terms whose members
# can appear in the periods this parliament is set up for.
OTHERS_SINCE = "2018-01-01"

_PERSON_FIELDS = """
  OPTIONAL { ?person rdfs:label ?it  FILTER(LANG(?it) = "it") }
  OPTIONAL { ?person rdfs:label ?mul FILTER(LANG(?mul) = "mul") }
  OPTIONAL { ?person rdfs:label ?en  FILTER(LANG(?en) = "en") }
  OPTIONAL { ?person skos:altLabel ?alias FILTER(LANG(?alias) IN ("it", "mul")) }
  OPTIONAL { ?person wdt:P1341 ?chamber_id }
"""
_PERSON_SELECT = """SELECT ?person (SAMPLE(?it) AS ?label_it) (SAMPLE(?mul) AS ?label_mul) (SAMPLE(?en) AS ?label_en)
       (GROUP_CONCAT(DISTINCT ?alias; separator="|") AS ?aliases)
       (GROUP_CONCAT(DISTINCT ?chamber_id; separator="|") AS ?chamber_ids) (MAX(?start) AS ?latest)"""

# p:/ps: and not wdt:, so that a statement of non-preferred rank still counts.
SPARQL_DEPUTIES = _PERSON_SELECT + """ WHERE {
  ?person p:P39 ?statement .
  ?statement ps:P39 wd:Q18558478 .
  OPTIONAL { ?statement pq:P580 ?start }
""" + _PERSON_FIELDS + "} GROUP BY ?person"

SPARQL_SENATORS = _PERSON_SELECT + """ WHERE {
  ?person p:P39 ?statement .
  ?statement ps:P39 wd:Q13653224 ; pq:P580 ?start .
  FILTER(?start >= "%(since)sT00:00:00Z"^^xsd:dateTime)
""" + _PERSON_FIELDS + "} GROUP BY ?person"

# Q83307 minister, Q736559 state secretary; Q38 Italy, as the office's country or jurisdiction.
SPARQL_GOVERNMENT = _PERSON_SELECT + """ WHERE {
  ?person p:P39 ?statement .
  ?statement ps:P39 ?office ; pq:P580 ?start .
  ?office wdt:P279* ?kind .
  VALUES ?kind { wd:Q83307 wd:Q736559 }
  { ?office wdt:P17 wd:Q38 } UNION { ?office wdt:P1001 wd:Q38 }
  FILTER(?start >= "%(since)sT00:00:00Z"^^xsd:dateTime)
""" + _PERSON_FIELDS + "} GROUP BY ?person"

# ``label`` is the group's name on the WebTV page; ``aliases`` add the record's
# abbreviation and the party's own name.
IT_GROUPS: list[dict] = [
    {"id": "Q1757843", "label": "FRATELLI D'ITALIA", "aliases": ["FDI", "Fratelli d'Italia"]},
    {"id": "Q47729", "label": "PARTITO DEMOCRATICO - ITALIA DEMOCRATICA E PROGRESSISTA",
     "aliases": ["PD-IDP", "Partito Democratico"]},
    {"id": "Q85776185", "label": "LEGA - SALVINI PREMIER", "aliases": ["LEGA", "Lega per Salvini Premier"]},
    {"id": "Q47817", "label": "MOVIMENTO 5 STELLE", "aliases": ["M5S"]},
    {"id": "Q14924303", "label": "FORZA ITALIA - BERLUSCONI PRESIDENTE - PPE", "aliases": ["FI-PPE", "Forza Italia"]},
    {"id": "Q113168799", "label": "ALLEANZA VERDI E SINISTRA", "aliases": ["AVS"]},
    {"id": "Q63926676", "label": "AZIONE-POPOLARI EUROPEISTI RIFORMATORI-RENEW EUROPE",
     "aliases": ["AZ-PER-RE", "Azione"]},
    {"id": "Q67785862", "label": "ITALIA VIVA-CASA RIFORMISTA", "aliases": ["IV-CR", "Italia Viva"]},
    {"id": "Q113563752",
     "label": "NOI MODERATI (NOI CON L'ITALIA, CORAGGIO ITALIA, UDC E ITALIA AL CENTRO)-MAIE-CENTRO POPOLARE",
     "aliases": ["NM(N-C-U-I)M-CP", "Noi Moderati"]},
    {"id": "Q3117887", "label": "MISTO", "aliases": ["Gruppo misto"]},
]


def parse_persons(payload: dict) -> dict[str, dict]:
    """``{qid: {id, label, aliases, chamber_ids, latest}}`` from a person query's response; an item without a usable label is left out."""
    persons: dict[str, dict] = {}
    for b in (payload.get("results") or {}).get("bindings") or []:
        qid = (bind(b, "person") or "").rsplit("/", 1)[-1]
        names = [(bind(b, key) or "").strip() for key in ("label_it", "label_mul", "label_en")]
        label = next((n for n in names if n), "")
        if not qid or not label:
            continue
        # An alias of one word is a bare surname or a first name: no key for a person.
        aliases = [a.strip() for a in (bind(b, "aliases") or "").split("|") if len(a.split()) > 1]
        persons[qid] = {
            "id": qid,
            "label": label,
            "aliases": sorted({n for n in [*names, *aliases] if n} - {label}),
            "chamber_ids": sorted(i.strip() for i in (bind(b, "chamber_ids") or "").split("|") if i.strip()),
            "latest": bind(b, "latest") or "",
        }
    return persons


def _entity(person: dict, sub_type: str) -> dict:
    entity = {"id": person["id"], "label": person["label"], "labelAlternative": person["aliases"],
              "type": "person", "subType": sub_type}
    if person["chamber_ids"]:
        ids = person["chamber_ids"]
        entity["additionalInformation"] = {"originPersonID": ids[0] if len(ids) == 1 else ids}
    return entity


def build_entities(deputies: dict[str, dict], others: dict[str, dict]) -> dict:
    """The entity document from the deputies, the other people who speak in the Chamber, and the group list."""
    data: list[dict] = []
    for person in sorted(deputies.values(), key=lambda p: (p["latest"], p["id"])):
        data.append(_entity(person, "memberOfParliament"))
    outside = [p for qid, p in others.items() if qid not in deputies]
    for person in sorted(outside, key=lambda p: (p["latest"], p["id"])):
        data.append(_entity(person, "person"))
    for group in IT_GROUPS:
        data.append({"id": group["id"], "label": group["label"], "labelAlternative": group["aliases"],
                     "type": "organisation", "subType": "faction"})
    with_id = sum(1 for p in deputies.values() if p["chamber_ids"])
    logger.info(f"Built {len(deputies)} deputies ({with_id} with the Chamber's id), "
                f"{len(outside)} senators and ministers who never sat in the Chamber, {len(IT_GROUPS)} groups")
    return {
        "meta": {"source": "Wikidata SPARQL (P39 Q18558478 deputies with P1341; P39 Q13653224 senators and "
                           f"offices of minister of Italy since {OTHERS_SINCE}) and the group list of this module"},
        "data": data,
    }


def write_entity_dump(config) -> Path:
    logger.info("Querying Wikidata for deputies, senators and ministers…")
    deputies = parse_persons(sparql_get(SPARQL_DEPUTIES))
    others = parse_persons(sparql_get(SPARQL_SENATORS % {"since": OTHERS_SINCE}))
    others.update(parse_persons(sparql_get(SPARQL_GOVERNMENT % {"since": OTHERS_SINCE})))
    doc = build_entities(deputies, others)
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
