#! /usr/bin/env python3
"""Build ``metadata/entities.json`` for the FI NEL stage.

Two sources, joined on the member's name:

1. **avoindata ``MemberOfParliament``** — the authoritative roster
   (``personId`` = henkilönumero, ``firstname``, ``lastname``, ``party``). This
   is the set of people we emit, so NEL only ever resolves actual MPs.
2. **Wikidata SPARQL** — everyone who has held ``P39 wd:Q17592486`` ("member of
   the Parliament of Finland"), giving the QID plus fi/sv/en label variants.
   Joined to the roster by normalised ``"Firstname Lastname"``.

Parties come from a fixed list keyed on the group abbreviation that appears in
the PTK ``LisatietoTeksti`` / the broadcast ``party.fi`` field (ps, kok, sd,
kesk, vihr, vas, r, kd, liik).

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
    __package__ = "optv.parliaments.FI.scraper"

from optv.parliaments.FI.common import Config
from optv.parliaments.FI.scraper.avoindata import _get_json, API_ROOT

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

# P39 wd:Q17592486 = "member of the Parliament of Finland". p:/ps: (not wdt:)
# so deprecated-rank membership statements (e.g. people who later joined the
# cabinet) are still followed — once an MP, always a former MP.
SPARQL_FI_MEMBERS = """
SELECT DISTINCT ?person ?label_fi ?label_sv ?label_en WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q17592486 .
  OPTIONAL { ?person rdfs:label ?label_fi FILTER(LANG(?label_fi)="fi") }
  OPTIONAL { ?person rdfs:label ?label_sv FILTER(LANG(?label_sv)="sv") }
  OPTIONAL { ?person rdfs:label ?label_en FILTER(LANG(?label_en)="en") }
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


def _sparql_get(query: str, *, timeout: float = 120.0) -> dict:
    url = SPARQL_ENDPOINT + "?" + urlencode({"query": query})
    req = Request(url, headers={"Accept": "application/sparql-results+json",
                                "User-Agent": USER_AGENT})
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


def _bind(b: dict, key: str) -> str | None:
    v = b.get(key)
    return v.get("value") if isinstance(v, dict) else None


def fetch_wikidata_members() -> dict[str, dict]:
    """Return ``{cleaned_name: {qid, label, aliases}}`` for FI MPs on Wikidata."""
    logger.info("Querying Wikidata for members of the Parliament of Finland…")
    payload = _sparql_get(SPARQL_FI_MEMBERS)
    bindings = (payload.get("results") or {}).get("bindings") or []
    by_name: dict[str, dict] = {}
    for b in bindings:
        iri = _bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        primary = _bind(b, "label_fi") or _bind(b, "label_en") or _bind(b, "label_sv")
        if not primary:
            continue
        aliases = []
        for k in ("label_fi", "label_sv", "label_en"):
            v = _bind(b, k)
            if v and v != primary and v not in aliases:
                aliases.append(v)
        for name in [primary, *aliases]:
            key = _clean(name)
            if key and key not in by_name:
                by_name[key] = {"qid": qid, "label": primary, "aliases": aliases}
    logger.info(f"Wikidata returned {len({v['qid'] for v in by_name.values()})} distinct members "
                f"({len(by_name)} name keys).")
    return by_name


def fetch_roster() -> list[dict]:
    """avoindata MemberOfParliament: list of ``{personId, firstname, lastname, party}``."""
    logger.info("Fetching MemberOfParliament roster from avoindata…")
    rows: list[dict] = []
    page = 0
    while True:
        qs = urlencode({"perPage": 100, "page": page})
        payload = _get_json(f"{API_ROOT}/MemberOfParliament/rows?{qs}")
        cols = payload.get("columnNames") or []
        rows.extend(dict(zip(cols, r)) for r in (payload.get("rowData") or []))
        if not payload.get("hasMore"):
            break
        page += 1
    logger.info(f"Roster: {len(rows)} members.")
    return rows


def build_entities() -> dict:
    wd_by_name = fetch_wikidata_members()
    roster = fetch_roster()

    data: list[dict] = []
    seen_qids: set[str] = set()
    matched = 0
    persons: "OrderedDict[str, dict]" = OrderedDict()
    for m in roster:
        first = (m.get("firstname") or "").strip()
        last = (m.get("lastname") or "").strip()
        name = f"{first} {last}".strip()
        if not name:
            continue
        key = _clean(name)
        wd = wd_by_name.get(key)
        qid = wd["qid"] if wd else ""
        if qid:
            matched += 1
        aliases = list(wd["aliases"]) if wd else []
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

    logger.info(f"Built {len(persons)} persons ({matched} with a Wikidata QID), "
                f"{len(FI_PARTIES)} parties.")
    return {
        "meta": {"source": "avoindata MemberOfParliament ⋈ Wikidata SPARQL (Q17592486) + party list"},
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
