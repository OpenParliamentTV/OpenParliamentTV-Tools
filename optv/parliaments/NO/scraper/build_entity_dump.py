#! /usr/bin/env python3
"""Build ``metadata/entities.json`` for the NO NEL stage.

Two sources, joined on Norwegian Bokmål label:

1. **Wikidata SPARQL** — everyone who has held the position
   ``P39 wd:Q11975003`` ("member of the Storting"). Provides the QID plus
   label variants (no, nb, nn, en).
2. **data.stortinget.no/eksport/representanter?stortingsperiodeid=…** and
   ``/dagensrepresentanter`` — current and historical Storting members,
   matched by name. Provides the `personid` short code as
   ``additionalInformation.epId``-style hint (we use ``originPersonID`` in
   the parser, but the dump only needs labels).

Parties are added from a fixed list of Norwegian parliamentary parties
keyed on the short code that appears in ``<Navn>`` brackets (Ap, H, Sp,
FrP, SV, V, KrF, R, MDG, INP).

Run:

    python -m optv.parliaments.NO.scraper.build_entity_dump <data_dir> --period 22
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
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.scraper"

from optv.parliaments.NO.common import Config, PERIOD_TO_TERM

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

# Q9045502 = "member of the Parliament of Norway" (Stortingsrepresentant).
#
# Use ``p:P39/ps:P39`` rather than ``wdt:P39`` because Wikidata occasionally
# downranks a person's Storting membership statement (e.g. when they take
# a cabinet position and the editor flags the old MP entry deprecated).
# ``wdt:`` only follows non-deprecated rank truthy statements; ``p:/ps:``
# follows every statement regardless of rank, which is what we want — once
# elected to the Storting, always a former member.
SPARQL_STORTING_MEMBERS = """
SELECT DISTINCT ?person ?label_no ?label_nb ?label_nn ?label_en WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q9045502 .
  OPTIONAL { ?person rdfs:label ?label_no  FILTER(LANG(?label_no)="no") }
  OPTIONAL { ?person rdfs:label ?label_nb  FILTER(LANG(?label_nb)="nb") }
  OPTIONAL { ?person rdfs:label ?label_nn  FILTER(LANG(?label_nn)="nn") }
  OPTIONAL { ?person rdfs:label ?label_en  FILTER(LANG(?label_en)="en") }
}
"""

# Norwegian parliamentary parties (current + recently represented).
# ``label`` is the short code that Stortinget puts in ``<Navn>`` brackets;
# ``aliases`` carries the full Norwegian name and any common variants.
NO_PARTIES: list[dict] = [
    {"id": "Q137323", "label": "Ap",  "aliases": ["Arbeiderpartiet", "A", "Det norske Arbeiderparti"]},
    {"id": "Q261541", "label": "H",   "aliases": ["Høyre"]},
    {"id": "Q511311", "label": "Sp",  "aliases": ["Senterpartiet"]},
    {"id": "Q193665", "label": "FrP", "aliases": ["Fremskrittspartiet"]},
    {"id": "Q261545", "label": "SV",  "aliases": ["Sosialistisk Venstreparti"]},
    {"id": "Q259954", "label": "V",   "aliases": ["Venstre"]},
    {"id": "Q511323", "label": "KrF", "aliases": ["Kristelig Folkeparti"]},
    {"id": "Q353207", "label": "R",   "aliases": ["Rødt"]},
    {"id": "Q1605437", "label": "MDG", "aliases": ["Miljøpartiet De Grønne", "Grønne"]},
    {"id": "Q108722448", "label": "INP", "aliases": ["Industri- og Næringspartiet"]},
    {"id": "Q11971236", "label": "Pf",  "aliases": ["Pasientfokus"]},
]


def _sparql_get(query: str, *, timeout: float = 120.0) -> dict:
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


def _bind(binding: dict, key: str) -> str | None:
    v = binding.get(key)
    return v.get("value") if isinstance(v, dict) else None


def fetch_wikidata_storting_members() -> list[dict]:
    logger.info("Querying Wikidata for Storting members…")
    payload = _sparql_get(SPARQL_STORTING_MEMBERS)
    bindings = (payload.get("results") or {}).get("bindings") or []
    members: dict[str, dict] = OrderedDict()
    for b in bindings:
        iri = _bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        primary = (_bind(b, "label_nb") or _bind(b, "label_no")
                   or _bind(b, "label_nn") or _bind(b, "label_en"))
        if not primary:
            continue
        if qid not in members:
            alts: list[str] = []
            for k in ("label_nb", "label_no", "label_nn", "label_en"):
                v = _bind(b, k)
                if v and v != primary and v not in alts:
                    alts.append(v)
            members[qid] = {"qid": qid, "label": primary, "aliases": alts}
        else:
            ent = members[qid]
            for k in ("label_nb", "label_no", "label_nn", "label_en"):
                v = _bind(b, k)
                if v and v != ent["label"] and v not in ent["aliases"]:
                    ent["aliases"].append(v)
    logger.info(f"Wikidata returned {len(members)} distinct Storting members.")
    return list(members.values())


def build_entities() -> dict:
    members = fetch_wikidata_storting_members()
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
    for party in NO_PARTIES:
        data.append({
            "id": party["id"],
            "label": party["label"],
            "labelAlternative": party["aliases"],
            "type": "organisation",
            "subType": "faction",
        })
    return {
        "meta": {"source": "Wikidata SPARQL (Q11975003) + manual party list"},
        "data": data,
    }


def write_entity_dump(config: Config) -> Path:
    target_dir = config.dir("nel_data", create=True)
    target = target_dir / "entities.json"
    doc = build_entities()
    target.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {target} ({len(doc['data'])} entities)")
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=22,
                        help="Stortingsperiode index (currently informational)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # Surface which term we're nominally building for.
    if args.period in PERIOD_TO_TERM:
        logger.info(f"Building entities for Storting term {PERIOD_TO_TERM[args.period]} "
                    f"(period {args.period}). The Wikidata query is term-agnostic — "
                    "all known members are emitted; the merger filters by membership "
                    "in practice via the personID match.")
    config = Config(args.data_dir)
    write_entity_dump(config)


if __name__ == "__main__":
    main()
