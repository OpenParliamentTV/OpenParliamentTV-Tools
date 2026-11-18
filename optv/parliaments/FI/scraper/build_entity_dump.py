#! /usr/bin/env python3
"""Build ``metadata/entities.json`` for the FI NEL stage.

Two sources, joined on the member's name:

1. **The open data API's MP register** (``/api/v1/reference-data/kansanedustajat``)
   — the authoritative roster (``tunnus`` = henkilönumero, ``nimi``,
   ``sukunimi``). This is the set of people we emit, so NEL only ever resolves
   actual MPs.
2. **Wikidata SPARQL** — everyone who has held ``P39 wd:Q17592486`` ("member of
   the Parliament of Finland"), giving the QID plus fi/sv/en/mul label
   variants. Joined to the roster by the Finnish MP ID (``P2181`` =
   henkilönumero), else by normalised ``"Firstname Lastname"`` — the ID join
   keeps MPs whose name on Wikidata differs from the register (name changes).

Parties come from a fixed list keyed on the group abbreviation that appears in
the speech records' ``puhuja.lisatieto`` / the broadcast ``party.fi`` field
(ps, kok, sd, kesk, vihr, vas, r, kd, liik).

The output matches ``optv.shared.nel.get_nel_data``: persons keyed by cleaned
label (subType ``memberOfParliament``) and factions keyed by cleaned label.

Run:
    python -m optv.parliaments.FI.scraper.build_entity_dump <data_dir>
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import unicodedata
from collections import OrderedDict
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.FI.scraper"

from optv.shared.entity_dump_bootstrap import bind as _bind, sparql_get as _sparql_get
from optv.parliaments.FI.common import Config
from optv.parliaments.FI.scraper.eduskunta_api import fetch_mp_reference

logger = logging.getLogger(__name__)


# P39 wd:Q17592486 = "member of the Parliament of Finland". p:/ps: (not wdt:)
# so deprecated-rank membership statements (e.g. people who later joined the
# cabinet) are still followed — once an MP, always a former MP.
SPARQL_FI_MEMBERS = """
SELECT DISTINCT ?person ?label_fi ?label_sv ?label_en ?label_mul ?mpid WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q17592486 .
  OPTIONAL { ?person rdfs:label ?label_fi FILTER(LANG(?label_fi)="fi") }
  OPTIONAL { ?person rdfs:label ?label_sv FILTER(LANG(?label_sv)="sv") }
  OPTIONAL { ?person rdfs:label ?label_en FILTER(LANG(?label_en)="en") }
  OPTIONAL { ?person rdfs:label ?label_mul FILTER(LANG(?label_mul)="mul") }
  OPTIONAL { ?person wdt:P2181 ?mpid }
}
"""

# Eduskunta parliamentary groups (current + recently represented). ``label`` is
# the abbreviation in the source data; ``aliases`` adds the full fi/sv names.
FI_PARTIES: list[dict] = [
    {"id": "Q634277", "label": "ps",   "aliases": ["Perussuomalaiset", "Sannfinländarna", "Finns Party", "PS"]},
    {"id": "Q304191", "label": "kok",  "aliases": ["Kansallinen Kokoomus", "Kokoomus", "Samlingspartiet", "National Coalition Party", "KOK"]},
    {"id": "Q499029", "label": "sd",   "aliases": ["Suomen Sosialidemokraattinen Puolue", "SDP", "Socialdemokraterna", "Social Democratic Party of Finland", "sdp"]},
    {"id": "Q506591", "label": "kesk", "aliases": ["Suomen Keskusta", "Keskusta", "Centern", "Centre Party", "KESK"]},
    {"id": "Q196695", "label": "vihr", "aliases": ["Vihreä liitto", "Vihreät", "Gröna förbundet", "Green League", "VIHR"]},
    {"id": "Q385927", "label": "vas",  "aliases": ["Vasemmistoliitto", "Vänsterförbundet", "Left Alliance", "VAS"]},
    {"id": "Q845537", "label": "r",    "aliases": ["Suomen ruotsalainen kansanpuolue", "RKP", "Svenska folkpartiet", "Swedish People's Party of Finland", "rkp"]},
    {"id": "Q1138982", "label": "kd",  "aliases": ["Kristillisdemokraatit", "Kristdemokraterna", "Christian Democrats", "KD"]},
    {"id": "Q52157683", "label": "liik", "aliases": ["Liike Nyt", "Rörelsen Nu", "Movement Now", "li", "liike"]},
]


def _clean(s: str) -> str:
    """Mirror ``optv.shared.nel.cleanup`` closely enough for join keys."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().split())


_LABEL_KEYS = ("label_fi", "label_sv", "label_en", "label_mul")


def fetch_wikidata_members() -> tuple[dict[str, dict], dict[str, dict]]:
    """Return ``(by_name, by_mpid)`` for FI MPs on Wikidata.

    Both map to ``{qid, label, aliases}``: ``by_name`` is keyed by cleaned label,
    ``by_mpid`` by the Finnish MP ID (``P2181``, the henkilönumero).
    """
    logger.info("Querying Wikidata for members of the Parliament of Finland…")
    payload = _sparql_get(SPARQL_FI_MEMBERS)
    bindings = (payload.get("results") or {}).get("bindings") or []
    by_name: dict[str, dict] = {}
    by_mpid: dict[str, dict] = {}
    for b in bindings:
        iri = _bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        # ``mul`` is the language-independent default label; some items have
        # no fi/sv/en label at all.
        primary = (_bind(b, "label_fi") or _bind(b, "label_en")
                   or _bind(b, "label_sv") or _bind(b, "label_mul"))
        if not primary:
            continue
        aliases = []
        for k in _LABEL_KEYS:
            v = _bind(b, k)
            if v and v != primary and v not in aliases:
                aliases.append(v)
        entry = {"qid": qid, "label": primary, "aliases": aliases}
        for name in [primary, *aliases]:
            key = _clean(name)
            if key and key not in by_name:
                by_name[key] = entry
        mpid = (_bind(b, "mpid") or "").strip()
        if mpid and mpid not in by_mpid:
            by_mpid[mpid] = entry
    logger.info(f"Wikidata returned {len({v['qid'] for v in by_name.values()})} distinct members "
                f"({len(by_name)} name keys, {len(by_mpid)} MP IDs).")
    return by_name, by_mpid


def fetch_roster() -> list[dict]:
    """The open data API's MP register as ``[{personId, firstname, lastname}]``."""
    logger.info("Fetching the MP register from api.eduskunta.fi…")
    rows = [{"personId": r.get("tunnus"), "firstname": r.get("nimi"),
             "lastname": r.get("sukunimi")}
            for r in fetch_mp_reference()]
    logger.info(f"Roster: {len(rows)} members.")
    return rows


def build_entities() -> dict:
    wd_by_name, wd_by_mpid = fetch_wikidata_members()
    roster = fetch_roster()

    data: list[dict] = []
    seen_qids: set[str] = set()
    persons: "OrderedDict[str, dict]" = OrderedDict()
    for m in roster:
        first = (m.get("firstname") or "").strip()
        last = (m.get("lastname") or "").strip()
        name = f"{first} {last}".strip()
        if not name:
            continue
        key = _clean(name)
        wd = wd_by_mpid.get(str(m.get("personId") or "")) or wd_by_name.get(key)
        qid = wd["qid"] if wd else ""
        aliases = []
        if wd:
            # The Wikidata name differs from the register after a name change.
            for alt in (wd["label"], *wd["aliases"]):
                if alt not in aliases and _clean(alt) != key:
                    aliases.append(alt)
        if last and first:
            for alt in (f"{last} {first}", last):
                if alt not in aliases and _clean(alt) != key:
                    aliases.append(alt)
        entry = persons.get(key)
        if entry is None:
            persons[key] = {
                "id": qid,
                "label": name,
                "labelAlternative": aliases,
                "type": "person",
                "subType": "memberOfParliament",
                "additionalInformation": {"personNumber": str(m.get("personId") or "")},
            }
        elif qid and not entry["id"]:
            entry["id"] = qid
    for entry in persons.values():
        if entry["id"]:
            seen_qids.add(entry["id"])
        data.append(entry)

    for party in FI_PARTIES:
        data.append({
            "id": party["id"],
            "label": party["label"],
            "labelAlternative": party["aliases"],
            "type": "organisation",
            "subType": "faction",
        })

    matched = sum(1 for entry in persons.values() if entry["id"])
    logger.info(f"Built {len(persons)} persons ({matched} with a Wikidata QID), "
                f"{len(FI_PARTIES)} parties.")
    return {
        "meta": {"source": "api.eduskunta.fi MP register ⋈ Wikidata SPARQL (Q17592486, P2181) + party list"},
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
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    write_entity_dump(config)


if __name__ == "__main__":
    main()
