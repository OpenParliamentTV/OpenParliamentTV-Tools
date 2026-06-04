#! /usr/bin/env python3
"""Build a NEL entity dump (entities.json) for EU term-10 MEPs and groups.

Inputs:
* data.europarl.europa.eu MEP list (term 10)
* Wikidata SPARQL: people with P1186 (European Parliament personal ID),
  joined against the MEP list by EP-ID to obtain QIDs and label aliases.

Output: ``<data_dir>/metadata/entities.json``, in the shape
``optv.shared.nel`` expects:

    {
      "meta": {"people_count": N, "factions_count": N, "total_count": N},
      "data": [
        {"id": "Q123", "label": "Name", "labelAlternative": [...],
         "firstname": "Given", "lastname": "Family",
         "type": "person", "subType": "memberOfParliament",
         "additionalInformation": {"epId": "28150"}},
        ...
        {"id": "Qxxx", "label": "European People's Party group",
         "labelAlternative": ["EPP Group", "PPE", ...],
         "type": "organisation", "subType": "faction"}
      ]
    }

Used by ``optv.shared.nel`` to attach Wikidata QIDs to speakers + factions.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

EU_MEPS_URL = "https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=10"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

UA = "optv-eu-entity-dump/0.1 (https://github.com/OpenParliamentTV)"

# Wikidata QIDs for term-10 political groups (verified 2025). These are
# the long-lived umbrella party entities, not the term-specific instances.
# The first alias in each list is the canonical merger label from
# parsers.common.EU_FACTION_LABELS — keeping them in sync ensures NEL's
# label-based lookup finds the QID for every speaker.
EU_FACTION_QIDS = {
    "PPE": ("Q16958101", "European People's Party Group",
            ["European People's Party",      # ← merger label (no "Group" suffix)
             "EPP Group", "PPE",
             "Group of the European People's Party"]),
    "S&D": ("Q1232865", "Progressive Alliance of Socialists and Democrats",
            ["Progressive Alliance of Socialists and Democrats",
             "S&D", "PASD",
             "Group of the Progressive Alliance of Socialists "
             "and Democrats in the European Parliament"]),
    "Renew": ("Q67054155", "Renew Europe",
              ["Renew Europe",
               "Renew Europe Group", "Renew", "RE"]),
    "Verts/ALE": ("Q1747031", "Greens/European Free Alliance",
                  ["The Greens / European Free Alliance",     # ← merger label
                   "Verts/ALE", "Greens/EFA",
                   "Group of the Greens / European Free Alliance"]),
    "ECR": ("Q857240", "European Conservatives and Reformists",
            ["European Conservatives and Reformists",
             "ECR", "ECR Group"]),
    "The Left": ("Q1331620", "The Left in the European Parliament – GUE/NGL",
                 ["The Left in the European Parliament - GUE/NGL",  # ← merger label (ASCII dash)
                  "GUE/NGL", "The Left",
                  "European United Left / Nordic Green Left"]),
    "PfE": ("Q127847519", "Patriots for Europe",
            ["Patriots for Europe",
             "PfE", "Patriots", "Patriots for Europe Group"]),
    "ESN": ("Q129180828", "Europe of Sovereign Nations",
            ["Europe of Sovereign Nations",
             "ESN", "Europe of Sovereign Nations Group"]),
    "NI": (None, "Non-attached Members",
           ["Non-attached Members",
            "NI", "Non-Inscrits", "Non-attached"]),
}


def fetch_meps() -> list[dict]:
    req = Request(EU_MEPS_URL, headers={
        "Accept": "application/ld+json",
        "User-Agent": UA,
    })
    with urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read())
    return payload.get("data") or []


SPARQL_QUERY = """
SELECT ?item ?epId ?itemLabel ?birthName ?nativeName WHERE {
  ?item wdt:P1186 ?epId .
  OPTIONAL { ?item wdt:P1477 ?birthName }
  OPTIONAL { ?item wdt:P1559 ?nativeName }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en,fr,de,es,it,pl,sv,nl,da,fi,cs,hu,ro,el,bg,sk,sl,hr,et,lt,lv,pt,ga,mt".
    ?item rdfs:label ?itemLabel .
  }
}
"""


def fetch_wikidata_p1186() -> dict[str, dict]:
    """Return {ep_id_str: {qid, label, aliases:[...]}} for all WD people with P1186."""
    url = WIKIDATA_SPARQL + "?" + urlencode({"query": SPARQL_QUERY, "format": "json"})
    req = Request(url, headers={
        "Accept": "application/sparql-results+json",
        "User-Agent": UA,
    })
    logger.info("querying Wikidata SPARQL for P1186 entries…")
    with urlopen(req, timeout=120) as resp:
        payload = json.loads(resp.read())
    rows = payload.get("results", {}).get("bindings", []) or []
    by_ep = {}
    for r in rows:
        ep_id = r.get("epId", {}).get("value", "").strip()
        qid_uri = r.get("item", {}).get("value", "")
        qid = qid_uri.rsplit("/", 1)[-1] if qid_uri else None
        if not ep_id or not qid:
            continue
        label = r.get("itemLabel", {}).get("value", "")
        aliases = set()
        for k in ("birthName", "nativeName"):
            v = r.get(k, {}).get("value", "")
            if v:
                aliases.add(v)
        cur = by_ep.get(ep_id)
        if cur:
            cur["aliases"].update(aliases)
            if label and label != cur["label"]:
                cur["aliases"].add(label)
        else:
            by_ep[ep_id] = {"qid": qid, "label": label, "aliases": aliases}
    logger.info(f"Wikidata returned {len(by_ep)} unique EP-IDs with QIDs")
    return by_ep


def fetch_aliases_for_qid(qid: str) -> list[str]:
    """Fetch alternative labels (skos:altLabel) for a single QID. Used for
    factions where we want all language variants."""
    query = (
        f"SELECT ?alias WHERE {{ wd:{qid} skos:altLabel ?alias . "
        f'FILTER (LANG(?alias) IN ("en","fr","de","es","it","pl","sv","nl",'
        f'"da","fi","cs","hu","ro","el","bg","sk","sl","hr","et","lt","lv","pt")) }}'
    )
    url = WIKIDATA_SPARQL + "?" + urlencode({"query": query, "format": "json"})
    req = Request(url, headers={"User-Agent": UA, "Accept": "application/sparql-results+json"})
    try:
        with urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read())
        return [r["alias"]["value"] for r in payload.get("results", {}).get("bindings", [])]
    except Exception as e:
        logger.warning(f"failed to fetch aliases for {qid}: {e}")
        return []


def build_mep_records(meps: list[dict], wikidata: dict[str, dict]) -> list[dict]:
    out = []
    matched = unmatched = 0
    for mep in meps:
        ep_id = mep.get("identifier") or ""
        wd = wikidata.get(ep_id)
        if not wd:
            unmatched += 1
            # Still include the MEP, just without a QID — NEL can match by
            # label/alias even if the QID is missing.
            out.append({
                "id": "",
                "label": mep.get("label") or "",
                "labelAlternative": [],
                "firstname": mep.get("givenName") or "",
                "lastname": mep.get("familyName") or "",
                "type": "person",
                "subType": "memberOfParliament",
                "additionalInformation": {"epId": ep_id},
            })
            continue
        matched += 1
        aliases = sorted(a for a in wd["aliases"] if a != wd["label"])
        # Also add the EP label form if different.
        ep_label = mep.get("label") or ""
        if ep_label and ep_label != wd["label"]:
            aliases.insert(0, ep_label)
        out.append({
            "id": wd["qid"],
            "label": wd["label"] or ep_label,
            "labelAlternative": aliases,
            "firstname": mep.get("givenName") or "",
            "lastname": mep.get("familyName") or "",
            "type": "person",
            "subType": "memberOfParliament",
            "additionalInformation": {"epId": ep_id},
        })
    logger.info(f"MEPs matched to Wikidata: {matched}/{matched+unmatched}")
    return out


def build_faction_records() -> list[dict]:
    out = []
    for abbr, (qid, label, aliases) in EU_FACTION_QIDS.items():
        record = {
            "id": qid or "",
            "label": label,
            "labelAlternative": aliases,
            "type": "organisation",
            "subType": "faction",
            "additionalInformation": {"factionAbbr": abbr},
        }
        if qid:
            # Augment with skos:altLabels from Wikidata in all EU languages
            extra = fetch_aliases_for_qid(qid)
            if extra:
                # de-dupe while preserving order
                seen = set(record["labelAlternative"])
                for a in extra:
                    if a not in seen:
                        record["labelAlternative"].append(a)
                        seen.add(a)
            time.sleep(0.5)   # be nice to WDQS
        out.append(record)
    return out


def build_dump(data_dir: Path) -> Path:
    meps = fetch_meps()
    logger.info(f"fetched {len(meps)} MEPs from EP API (term 10)")
    wd = fetch_wikidata_p1186()
    mep_records = build_mep_records(meps, wd)
    faction_records = build_faction_records()
    dump = {
        "meta": {
            "people_count": len(mep_records),
            "factions_count": len(faction_records),
            "total_count": len(mep_records) + len(faction_records),
            "source": "data.europarl.europa.eu term=10 + Wikidata SPARQL P1186",
        },
        "data": mep_records + faction_records,
    }
    metadata_dir = data_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    out_path = metadata_dir / "entities.json"
    out_path.write_text(json.dumps(dump, indent=2, ensure_ascii=False))
    logger.info(f"wrote {out_path} "
                f"({len(mep_records)} MEPs, {len(faction_records)} factions)")
    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path, help="OPTV data directory root")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    build_dump(args.data_dir)


if __name__ == "__main__":
    main()
