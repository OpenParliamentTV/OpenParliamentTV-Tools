#! /usr/bin/env python3
"""Build the AT NEL entity dump (``metadata/entities.json``) from Wikidata.

The shared NEL stage (:func:`optv.shared.nel.get_nel_data`) reads
``metadata/entities.json`` — a ``{"data": [ent, …]}`` file whose entries carry a
``label`` + ``labelAlternative`` matched (cleaned, accent-folded, lowercased)
against each speech's ``people[].label`` / ``faction.label`` to attach a
Wikidata ``id``.

Two sources:
- **memberOfParliament** — every member of the 27th National Council term
  (position ``Q17535155`` qualified by parliamentary term ``Q69340785``), via a
  SPARQL query against the Wikidata Query Service.
- **faction** — the parliamentary clubs of the XXVII. GP, hard-coded with the
  label variants the stenographic protocols use (``ÖVP``, ``Grüne``, …).

Government members who are not MdNs (several AT federal ministers) are a known
coverage gap — entity-fishing NER picks up the prominent ones in fulltext.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

# QLever's Wikidata mirror is fast and unthrottled; the official WDQS endpoint
# is the fallback. Both speak standard SPARQL — the query avoids the WDQS-only
# `SERVICE wikibase:label` and reads `rdfs:label` directly so it runs on either.
ENDPOINTS = ["https://qlever.dev/api/wikidata", "https://query.wikidata.org/sparql"]
USER_AGENT = "OpenParliamentTV-Tools/1.0 (+https://github.com/OpenParliamentTV)"

POSITION_NR_MEMBER = "Q17535155"   # member of the National Council of Austria
TERM_XXVII = "Q69340785"           # 27. Gesetzgebungsperiode des österreichischen Nationalrats

# Parliamentary *clubs* (Parlamentsklubs) of the XXVII. GP — the OPTV faction is
# the parliamentary group, not the party, so these are the Klub Wikidata items
# (cf. the original AT prototype's klubs.json). labelAlternative carries every
# spelling the protocols use; cleanup() folds accents+case so "ÖVP"→"ovp",
# "Grüne"→"grune".
FACTIONS = [
    {"id": "Q59617931", "label": "ÖVP",
     "labelAlternative": ["Parlamentsklub der Österreichischen Volkspartei",
                          "ÖVP-Parlamentsklub", "Österreichische Volkspartei", "OEVP"]},
    {"id": "Q37994784", "label": "SPÖ",
     "labelAlternative": ["SPÖ-Parlamentsklub", "Sozialdemokratische Partei Österreichs", "SPOE"]},
    {"id": "Q37994791", "label": "FPÖ",
     "labelAlternative": ["FPÖ-Parlamentsklub", "Klub der Freiheitlichen Partei",
                          "FPÖ-Nationalratsklub", "Freiheitliche Partei Österreichs", "FPOE"]},
    {"id": "Q37994797", "label": "NEOS",
     "labelAlternative": ["NEOS-Parlamentsklub", "Klub von NEOS", "NEOS – Das Neue Österreich"]},
    {"id": "Q37994795", "label": "Grüne",
     "labelAlternative": ["Grüner Parlamentsklub", "Die Grünen", "GRÜNE", "GRÜNEN",
                          "Die Grünen – Die Grüne Alternative"]},
]


# Federal government members of the XXVII. GP (Kurz II / Schallenberg / Nehammer
# cabinets) who frequently speak but are not MdNs, so they're absent from the
# membership query. Curated (Wikidata's cabinet-membership modelling is too
# inconsistent to query reliably). subType "person": fills NEL keys no MP claims.
MINISTERS = [
    ("Q2262885", "Sebastian Kurz"), ("Q42860944", "Karl Nehammer"),
    ("Q64168538", "Alexander Schallenberg"), ("Q2561778", "Werner Kogler"),
    ("Q64954602", "Leonore Gewessler"), ("Q42162193", "Alma Zadić"),
    ("Q15812048", "Gernot Blümel"), ("Q1884520", "Magnus Brunner"),
    ("Q1600191", "Heinz Faßmann"), ("Q61639165", "Martin Polaschek"),
    ("Q43380898", "Klaudia Tanner"), ("Q86851", "Elisabeth Köstinger"),
    ("Q34215724", "Norbert Totschnig"), ("Q80289609", "Susanne Raab"),
    ("Q36493675", "Margarete Schramböck"), ("Q18222522", "Martin Kocher"),
    ("Q2172341", "Rudolf Anschober"), ("Q106484746", "Wolfgang Mückstein"),
    ("Q1698215", "Johannes Rauch"), ("Q80610916", "Christine Aschbacher"),
    ("Q1511874", "Gerhard Karner"), ("Q45815517", "Karoline Edtstadler"),
    ("Q42294925", "Claudia Plakolm"),
]


def _sparql(query: str, timeout: int = 120) -> dict:
    last_exc = None
    for endpoint in ENDPOINTS:
        url = endpoint + "?" + urllib.parse.urlencode({"query": query})
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                   "Accept": "application/sparql-results+json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except Exception as e:  # noqa: BLE001 — try the next mirror
            logger.warning(f"SPARQL endpoint {endpoint} failed: {type(e).__name__}: {e}")
            last_exc = e
    raise RuntimeError(f"All SPARQL endpoints failed: {last_exc}")


def fetch_members(position: str, term: str) -> list[dict]:
    query = f"""
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX ps: <http://www.wikidata.org/prop/statement/>
    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    SELECT ?person ?label (GROUP_CONCAT(DISTINCT ?alt; SEPARATOR="|") AS ?alts) WHERE {{
      ?person p:P39 ?st . ?st ps:P39 wd:{position} . ?st pq:P2937 wd:{term} .
      ?person rdfs:label ?label . FILTER(LANG(?label) = "de")
      OPTIONAL {{ ?person skos:altLabel ?alt . FILTER(LANG(?alt) = "de") }}
    }} GROUP BY ?person ?label
    """
    rows = _sparql(query)["results"]["bindings"]
    members = []
    for r in rows:
        qid = r["person"]["value"].rsplit("/", 1)[-1]
        label = r.get("label", {}).get("value", "")
        if not label or label == qid:
            continue
        alts = [a for a in (r.get("alts", {}).get("value", "") or "").split("|") if a and a != label]
        members.append({
            "id": qid,
            "type": "person",
            "subType": "memberOfParliament",
            "label": label,
            "labelAlternative": sorted(set(alts)),
            "additionalInformation": {},
        })
    return members


def build(position: str, term: str) -> dict:
    members = fetch_members(position, term)
    logger.info(f"Fetched {len(members)} National Council members for term {term}")
    factions = [{**f, "type": "organisation", "subType": "faction",
                 "additionalInformation": {}} for f in FACTIONS]
    ministers = [{"id": qid, "type": "person", "subType": "person", "label": label,
                  "labelAlternative": [], "additionalInformation": {}}
                 for qid, label in MINISTERS]
    logger.info(f"Added {len(ministers)} curated government members (subType person)")
    return {"data": members + ministers + factions}


def main():
    parser = argparse.ArgumentParser(description="Build the AT NEL entity dump from Wikidata.")
    parser.add_argument("data_dir", type=Path, help="OpenParliamentTV-Data-AT root directory")
    parser.add_argument("--position", default=POSITION_NR_MEMBER)
    parser.add_argument("--term", default=TERM_XXVII)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    doc = build(args.position, args.term)
    out_dir = args.data_dir / "metadata"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "entities.json"
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} entities)")


if __name__ == "__main__":
    main()
