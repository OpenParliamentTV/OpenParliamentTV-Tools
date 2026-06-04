#! /usr/bin/env python3
"""Build ``metadata/entities.json`` for the DE-SH NEL stage.

Two sources, joined on German label:

1. **Wikidata SPARQL** — everyone who has held the position
   ``P39 wd:Q18130496`` ("member of the Landtag of Schleswig-Holstein").
   Provides the QID plus German and English label variants. Returns
   roughly 688 historical members (verified 2026-05-29).
2. **Hand-curated party list** for the parties currently represented in
   WP 20: CDU, Grüne, SPD, FDP, SSW. (AfD held seats in WP 19 but did
   not clear the 5% threshold in the 2022 election.) The SSW is the
   Danish-Frisian minority party which is exempt from the 5% threshold.

Run::

    python -m optv.parliaments.DE-SH.scraper.build_entity_dump <data_dir>

Until the hosted ``https://de-sh.openparliament.tv/data/entity-dump/``
exists this is the canonical source; the workflow falls back to it via
the local ``metadata/entities.json`` path picked up by
``optv.shared.nel.get_nel_data``.
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
from optv.shared.entity_dump_bootstrap import (
    sparql_get as _sparql_get,
    bind as _bind,
    SPARQL_ENDPOINT,
    USER_AGENT,
)

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-SH.scraper"

logger = logging.getLogger(__name__)

# Q18130496 = "member of the Landtag of Schleswig-Holstein"
# (Mitglied des Schleswig-Holsteinischen Landtags).
SPARQL_DE_SH_MPS = """
SELECT DISTINCT ?person ?label_de ?label_en WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q18130496 .
  OPTIONAL { ?person rdfs:label ?label_de  FILTER(LANG(?label_de)="de") }
  OPTIONAL { ?person rdfs:label ?label_en  FILTER(LANG(?label_en)="en") }
}
"""

# Parties currently represented in WP 20 (since June 2022): CDU, Grüne,
# SPD, FDP, SSW. AfD held seats in WP 19 only — left out here. The
# ``Regierung`` ``gruppe`` that m7k attaches to cabinet speakers is not
# a party, it's a structural marker and stays unlinked.
DE_SH_PARTIES: list[dict] = [
    {"id": "Q49762", "label": "CDU", "aliases": [
        "Christlich Demokratische Union Deutschlands",
    ]},
    {"id": "Q49768", "label": "SPD", "aliases": [
        "Sozialdemokratische Partei Deutschlands",
    ]},
    {"id": "Q13124", "label": "FDP", "aliases": [
        "Freie Demokratische Partei",
    ]},
    {"id": "Q49766", "label": "BÜNDNIS 90/DIE GRÜNEN", "aliases": [
        "Bündnis 90/Die Grünen",
        "GRÜNE",
        "Grüne",
        "B90/Grüne",
    ]},
    {"id": "Q161545", "label": "SSW", "aliases": [
        "Südschleswigscher Wählerverband",
        "Sydslesvigsk Vælgerforening",
    ]},
]


def fetch_wikidata_de_sh_mps() -> list[dict]:
    logger.info("Querying Wikidata for DE-SH Landtag members…")
    payload = _sparql_get(SPARQL_DE_SH_MPS)
    bindings = (payload.get("results") or {}).get("bindings") or []
    members: dict[str, dict] = OrderedDict()
    for b in bindings:
        iri = _bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        de = (_bind(b, "label_de") or "").strip()
        en = (_bind(b, "label_en") or "").strip()
        label = de or en or qid
        alts = [s for s in {en} - {label, ""} if s]
        members[qid] = {
            "id": qid,
            "label": label,
            "subType": "memberOfParliament",
            "labelAlternative": alts,
        }
    logger.info(f"Got {len(members)} MPs from Wikidata")
    return list(members.values())


def build_entity_dump(metadata_dir: Path) -> Path:
    metadata_dir = Path(metadata_dir)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    persons = fetch_wikidata_de_sh_mps()
    factions = [
        {
            "id": p["id"],
            "label": p["label"],
            "subType": "faction",
            "labelAlternative": p["aliases"],
        }
        for p in DE_SH_PARTIES
    ]

    payload = {"data": persons + factions}
    out = metadata_dir / "entities.json"
    with out.open("w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    logger.info(f"Wrote {out} ({len(persons)} persons, {len(factions)} factions)")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    build_entity_dump(args.data_dir / "metadata")
