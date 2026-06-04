#! /usr/bin/env python3
"""Build ``metadata/entities.json`` for the DE-NI NEL stage.

Two sources, joined on German label:

1. **Wikidata SPARQL** — everyone who has held the position
   ``P39 wd:Q17521638`` ("member of the Landtag of Lower Saxony" / Mitglied des
   Niedersächsischen Landtags). Provides the QID plus German and English label
   variants. The Plenar-TV API already names speakers ``name`` (given) +
   ``surname`` (family), and the parser joins them in natural ``Firstname
   Lastname`` order, so no reordering is needed for label matching.
2. **Hand-curated party list** for the factions represented in WP 19 (since the
   October 2022 election): SPD, CDU, Bündnis 90/Die Grünen, AfD. The faction
   label as it appears in the API (``data`` field ``fraktion``) is included as a
   label/alias so the cleaned-label NEL lookup hits.

Run::

    python -m optv.parliaments.DE-NI.scraper.build_entity_dump <data_dir>

Until the hosted ``https://de-ni.openparliament.tv/data/entity-dump/`` exists
this is the canonical source; the workflow falls back to it via the local
``metadata/entities.json`` path picked up by ``optv.shared.nel.get_nel_data``.

NEL coverage caveat (the DE-HH / DE-SH / DE-BY / DE-BW class): ``P39 wd:Q17521638``
misses current WP-19 members whose items lack the statement, and government
members (Ministerinnen/Minister) who are not sitting MdL. The API does expose a
stable ``abg_id`` per speaker — a future precise abg_id→Wikidata bridge would
close the residual gap; v1 stays name-based.
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
    __package__ = "optv.parliaments.DE-NI.scraper"

logger = logging.getLogger(__name__)

# Q17521638 = "member of the Landtag of Lower Saxony"
# (Mitglied des Niedersächsischen Landtags).
SPARQL_DE_NI_MPS = """
SELECT DISTINCT ?person ?label_de ?label_en WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q17521638 .
  OPTIONAL { ?person rdfs:label ?label_de  FILTER(LANG(?label_de)="de") }
  OPTIONAL { ?person rdfs:label ?label_en  FILTER(LANG(?label_en)="en") }
}
"""

# Factions represented in WP 19 (since the October 2022 election). The ``fraktion``
# string in the API (e.g. "CDU", "Bündnis 90/Die Grünen") is matched against
# label + aliases. Ministers and "fraktionslos" carry no party and stay unlinked.
DE_NI_PARTIES: list[dict] = [
    {"id": "Q49768", "label": "SPD", "aliases": [
        "Sozialdemokratische Partei Deutschlands",
    ]},
    {"id": "Q49762", "label": "CDU", "aliases": [
        "Christlich Demokratische Union Deutschlands",
        "CDU/CSU",
    ]},
    {"id": "Q49766", "label": "Bündnis 90/Die Grünen", "aliases": [
        "BÜNDNIS 90/DIE GRÜNEN",
        "GRÜNE",
        "Grüne",
        "B90/Grüne",
    ]},
    {"id": "Q6721203", "label": "AfD", "aliases": [
        "Alternative für Deutschland",
    ]},
]


def fetch_wikidata_de_ni_mps() -> list[dict]:
    logger.info("Querying Wikidata for DE-NI Landtag members…")
    payload = _sparql_get(SPARQL_DE_NI_MPS)
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

    persons = fetch_wikidata_de_ni_mps()
    factions = [
        {
            "id": p["id"],
            "label": p["label"],
            "subType": "faction",
            "labelAlternative": p["aliases"],
        }
        for p in DE_NI_PARTIES
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
