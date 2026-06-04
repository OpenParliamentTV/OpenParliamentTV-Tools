#! /usr/bin/env python3
"""Build ``metadata/entities.json`` (NEL) and ``metadata/acteurs.json`` for FR.

Two outputs, both derived from the AN "Acteurs, mandats et organes" open data
(``AMO10_deputes_actifs_mandats_actifs_organes``) joined with Wikidata:

1. **``entities.json``** — what ``optv.shared.nel`` reads: every active député
   (``subType: memberOfParliament``, keyed by name, with a Wikidata QID when one
   matches) plus the 12 active parliamentary groups (``subType: faction``).
2. **``acteurs.json``** — a ``{PA-id: {label, civilite, groupAbbrev,
   groupLabel}}`` map used by the proceedings parser to assign each speaker's
   group (the Syceron compte rendu names the speaker but not their groupe).

Wikidata join: members of the French National Assembly (``P39 wd:Q3044918``)
matched to the AMO roster by normalised "Prénom Nom" (and by the AN id ``P4123``
when present). Run::

    python -m optv.parliaments.FR.scraper.build_entity_dump <data_dir>
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
import unicodedata
import zipfile
from collections import OrderedDict
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.FR.scraper"

from optv.parliaments.FR.common import Config
from optv.parliaments.FR.scraper.common import USER_AGENT, http_get

logger = logging.getLogger(__name__)

AMO10_URL = ("https://data.assemblee-nationale.fr/static/openData/repository/"
             "{leg}/amo/deputes_actifs_mandats_actifs_organes/"
             "AMO10_deputes_actifs_mandats_actifs_organes.json.zip")

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
# P39 wd:Q3044918 = "member of the French National Assembly". p:/ps: so
# deprecated-rank (former) memberships are followed too — once a deputy,
# always a former deputy.
SPARQL_FR_MEMBERS = """
SELECT DISTINCT ?person ?label_fr ?label_en ?anid WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q3044918 .
  OPTIONAL { ?person rdfs:label ?label_fr FILTER(LANG(?label_fr)="fr") }
  OPTIONAL { ?person rdfs:label ?label_en FILTER(LANG(?label_en)="en") }
  OPTIONAL { ?person wdt:P4123 ?anid }
}
"""

# Manual override for parliamentary-group Wikidata QIDs (17e législature). Only
# verified values belong here — anything left out is resolved dynamically from
# Wikidata (see fetch_wikidata_groups); a group that resolves to nothing keeps
# an empty id, which NEL treats as a warning, not an error. This whole file is
# a temporary stand-in: once the FR platform is online, entities.json is served
# by a live pull of the platform's curated entities, so the shape must match
# the platform entity-dump (persons with firstname/lastname; factions as
# {id,label,labelAlternative,type:organisation,subType:faction}).
# 17e-législature groups, verified 2026-05-30 against the P4100 group qualifier
# (per-group current-deputy count cross-checked with the AMO roster). These are
# pinned because several groups were renamed in 2024 (EPR ← "groupe Renaissance",
# DR ← "Les Républicains") and Wikidata's labels lag, so label matching alone
# misses them. fetch_wikidata_groups() still resolves anything not pinned here.
GROUP_QIDS_OVERRIDE: dict[str, str] = {
    "RN": "Q112813164",       # groupe Rassemblement national
    "EPR": "Q30584989",       # groupe Renaissance → Ensemble pour la République
    "LFI-NFP": "Q30503094",   # groupe La France insoumise
    "SOC": "Q2236823",        # groupe socialiste
    "DR": "Q36435376",        # groupe Les Républicains → Droite républicaine
    "ECOS": "Q3117954",       # groupe écologiste
    "DEM": "Q30596612",       # groupe démocrate (MoDem et indépendants)
    "HOR": "Q112672999",      # groupe Horizons et apparentés
    "LIOT": "Q57540493",      # groupe Libertés, indépendants, outre-mer, territoires
    "GDR": "Q2450519",        # groupe Gauche démocrate et républicaine
    "UDDPLR": "Q127508429",   # groupe Union des droites pour la République
    "NI": "Q3044925",         # non-inscrit à l'Assemblée nationale
}

# P4100 = "parliamentary group" qualifier on the P39 "member of the French
# National Assembly" statement: the canonical, per-deputy link to the group
# item. Aggregating current (no end-date) statements yields the active groups.
SPARQL_FR_GROUPS = """
SELECT ?grp ?label_fr ?abbrev (COUNT(DISTINCT ?person) AS ?n) WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:Q3044918 .
  ?stmt pq:P4100 ?grp .
  FILTER NOT EXISTS { ?stmt pq:P582 ?end }
  OPTIONAL { ?grp rdfs:label ?label_fr FILTER(LANG(?label_fr)="fr") }
  OPTIONAL { ?grp wdt:P1813 ?abbrev FILTER(LANG(?abbrev)="fr") }
}
GROUP BY ?grp ?label_fr ?abbrev
ORDER BY DESC(?n)
"""

# Tokens stripped from a Wikidata group label before matching it to the AMO
# group label (Wikidata labels read "groupe X à l'Assemblée nationale").
_GROUP_NOISE = ("groupe", "a l assemblee nationale", "a l assemblee",
                "assemblee nationale", "parlementaire")


def _clean(s: str) -> str:
    """Mirror ``optv.shared.nel.cleanup`` closely enough for join keys."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().split())


# --------------------------------------------------------------------------- #
# AMO (Assemblée nationale acteurs / organes)
# --------------------------------------------------------------------------- #

def _load_amo(leg: int) -> tuple[dict[str, dict], dict[str, dict]]:
    """Download + parse AMO10. Returns ``(acteurs, gp_organes)``.

    ``acteurs``: ``{PA-id: {label, civilite, groupAbbrev, groupLabel}}``.
    ``gp_organes``: ``{abbrev: {label, abbrev}}`` for active parliamentary groups.
    """
    url = AMO10_URL.format(leg=leg)
    logger.info(f"downloading AMO10 roster {url}")
    blob = http_get(url, timeout=120, binary=True)

    organe_gp: dict[str, dict] = {}        # PO-id → {abbrev, label}
    acteur_raw: list[dict] = []
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            if "/organe/" in name and name.endswith(".json"):
                d = json.loads(zf.read(name))["organe"]
                if d.get("codeType") == "GP" and not (d.get("viMoDe") or {}).get("dateFin"):
                    po = _uid_text(d.get("uid"))
                    organe_gp[po] = {
                        "abbrev": d.get("libelleAbrev") or d.get("libelleAbrege") or "",
                        "label": d.get("libelle") or "",
                    }
            elif "/acteur/" in name and name.endswith(".json"):
                acteur_raw.append(json.loads(zf.read(name))["acteur"])

    acteurs: dict[str, dict] = {}
    for a in acteur_raw:
        pa = _uid_text(a.get("uid"))
        ident = (a.get("etatCivil") or {}).get("ident") or {}
        first = (ident.get("prenom") or "").strip()
        last = (ident.get("nom") or "").strip()
        civ = (ident.get("civ") or "").strip()
        gp_ref = _active_gp_ref(a)
        gp = organe_gp.get(gp_ref or "", {})
        acteurs[pa] = {
            "label": f"{first} {last}".strip(),
            "firstname": first,
            "lastname": last,
            "civilite": civ,
            "groupAbbrev": gp.get("abbrev", ""),
            "groupLabel": gp.get("label", ""),
        }
    gp_by_abbrev = {v["abbrev"]: v for v in organe_gp.values() if v["abbrev"]}
    logger.info(f"AMO10: {len(acteurs)} deputies, {len(gp_by_abbrev)} active groups")
    return acteurs, gp_by_abbrev


def _uid_text(uid) -> str:
    if isinstance(uid, dict):
        return uid.get("#text") or ""
    return uid or ""


def _active_gp_ref(acteur: dict) -> str | None:
    """The organeRef of the deputy's active (open-ended) GP mandate."""
    mandats = ((acteur.get("mandats") or {}).get("mandat")) or []
    if isinstance(mandats, dict):
        mandats = [mandats]
    fallback = None
    for m in mandats:
        if m.get("typeOrgane") != "GP":
            continue
        ref = (m.get("organes") or {}).get("organeRef")
        if isinstance(ref, list):
            ref = ref[0] if ref else None
        fallback = fallback or ref
        if not m.get("dateFin"):
            return ref
    return fallback


# --------------------------------------------------------------------------- #
# Wikidata
# --------------------------------------------------------------------------- #

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


def fetch_wikidata_members() -> tuple[dict[str, str], dict[str, str]]:
    """Return ``(by_name, by_anid)`` mapping to QID for FR National Assembly MPs."""
    logger.info("Querying Wikidata for members of the French National Assembly…")
    try:
        payload = _sparql_get(SPARQL_FR_MEMBERS)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Wikidata query failed ({e}); persons will have no QID")
        return {}, {}
    bindings = (payload.get("results") or {}).get("bindings") or []
    by_name: dict[str, str] = {}
    by_anid: dict[str, str] = {}
    for b in bindings:
        iri = _bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        for k in ("label_fr", "label_en"):
            label = _bind(b, k)
            if label:
                by_name.setdefault(_clean(label), qid)
        anid = _bind(b, "anid")
        if anid:
            by_anid.setdefault(anid, qid)
            by_anid.setdefault(f"PA{anid}", qid)
    logger.info(f"Wikidata: {len({*by_name.values()})} distinct members "
                f"({len(by_name)} name keys, {len(by_anid)} AN-id keys)")
    return by_name, by_anid


def _norm_group(label: str) -> str:
    """Normalise a group label for matching (drop accents/noise words)."""
    s = _clean(label)
    for noise in _GROUP_NOISE:
        s = s.replace(noise, " ")
    # & ↔ et
    s = s.replace(" et ", " ").replace("&", " ")
    return " ".join(s.split())


def fetch_wikidata_groups() -> list[dict]:
    """Return ``[{qid, label, abbrev, n}]`` for the active AN parliamentary groups."""
    logger.info("Querying Wikidata for active National Assembly groups…")
    try:
        payload = _sparql_get(SPARQL_FR_GROUPS)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Wikidata groups query failed ({e}); factions will have no QID")
        return []
    out: list[dict] = []
    for b in (payload.get("results") or {}).get("bindings") or []:
        iri = _bind(b, "grp") or ""
        if not iri:
            continue
        out.append({
            "qid": iri.rsplit("/", 1)[-1],
            "label": _bind(b, "label_fr") or "",
            "abbrev": _bind(b, "abbrev") or "",
            "n": int(_bind(b, "n") or 0),
        })
    logger.info(f"Wikidata: {len(out)} active parliamentary groups")
    return out


def _resolve_group_qid(abbrev: str, label: str, wd_groups: list[dict]) -> str:
    """Best-effort QID for an AMO group. Conservative: never guess wrong.

    Priority: manual override → exact Wikidata short-name (P1813) match →
    normalised-label containment (the AMO label is a substring of the Wikidata
    label or vice versa). Anything ambiguous stays empty.
    """
    if GROUP_QIDS_OVERRIDE.get(abbrev):
        return GROUP_QIDS_OVERRIDE[abbrev]
    ab = abbrev.strip().lower()
    for g in wd_groups:
        if g["abbrev"] and g["abbrev"].strip().lower() == ab:
            return g["qid"]
    target = _norm_group(label)
    if not target:
        return ""
    for g in wd_groups:
        cand = _norm_group(g["label"])
        if cand and (cand == target or cand in target or target in cand):
            return g["qid"]
    return ""


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #

def build(leg: int) -> tuple[dict, dict]:
    acteurs, gp_by_abbrev = _load_amo(leg)
    wd_by_name, wd_by_anid = fetch_wikidata_members()
    wd_groups = fetch_wikidata_groups()

    data: list[dict] = []
    persons: "OrderedDict[str, dict]" = OrderedDict()
    matched = 0
    for pa, info in sorted(acteurs.items()):
        name = info["label"]
        if not name:
            continue
        key = _clean(name)
        qid = wd_by_anid.get(pa) or wd_by_name.get(key) or ""
        if qid:
            matched += 1
        aliases = []
        first, last = info["firstname"], info["lastname"]
        if first and last:
            aliases.append(f"{last} {first}")
        entry = persons.get(key)
        if entry is None:
            person = {
                "id": qid,
                "label": name,
                "labelAlternative": aliases,
                # firstname/lastname mirror the platform entity-dump shape (see
                # de.openparliament.tv/data/entity-dump) so the live pull that
                # will eventually replace this file is a drop-in.
                "firstname": first,
                "lastname": last,
                "type": "person",
                "subType": "memberOfParliament",
                "additionalInformation": {"idActeur": pa},
            }
            persons[key] = person
        elif qid and not entry["id"]:
            entry["id"] = qid
    data.extend(persons.values())

    groups_with_qid = 0
    for abbrev, gp in sorted(gp_by_abbrev.items()):
        qid = _resolve_group_qid(abbrev, gp["label"], wd_groups)
        if qid:
            groups_with_qid += 1
        data.append({
            "id": qid,
            "label": abbrev,
            "labelAlternative": [gp["label"]] if gp["label"] else [],
            "type": "organisation",
            "subType": "faction",
        })

    logger.info(f"Built {len(persons)} persons ({matched} with QID), "
                f"{len(gp_by_abbrev)} groups ({groups_with_qid} with QID)")
    entities = {
        "meta": {"source": "AMO10 deputes_actifs ⋈ Wikidata SPARQL "
                           "(P39 Q3044918 persons; P4100 groups)"},
        "data": data,
    }
    acteurs_doc = {
        "meta": {"source": "AMO10 deputes_actifs_mandats_actifs_organes",
                 "legislature": leg},
        "acteurs": acteurs,
    }
    return entities, acteurs_doc


def write_entity_dump(config: Config, leg: int = 17) -> Path:
    target_dir = config.dir("nel_data", create=True)
    entities, acteurs_doc = build(leg)
    entities_path = target_dir / "entities.json"
    acteurs_path = target_dir / "acteurs.json"
    entities_path.write_text(json.dumps(entities, indent=2, ensure_ascii=False))
    acteurs_path.write_text(json.dumps(acteurs_doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {entities_path} ({len(entities['data'])} entities) "
                f"and {acteurs_path} ({len(acteurs_doc['acteurs'])} acteurs)")
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
