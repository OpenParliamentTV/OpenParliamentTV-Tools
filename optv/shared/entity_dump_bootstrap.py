#! /usr/bin/env python3
"""Local entity dumps for parliaments whose platform does not serve one yet.

The NEL stage links speakers and factions against ``metadata/entities.json``. A parliament with a platform instance gets that file from its manifest ``entity_dump_url`` (as DE does); the others build it locally from Wikidata. This module holds the Wikidata SPARQL plumbing those builders share, and the complete builder for parliaments that need nothing more than "everyone who held one position, plus a fixed party list" (the German Landtage)::

    python -m optv.shared.entity_dump_bootstrap DE-BW <data_dir>

That builder is configured in the parliament's ``manifest.yaml``::

    entity_bootstrap:
      wikidata_position: Q17481175      # P39 value: member of this parliament
      wikidata_aliases: false           # also take multi-word German aliases of a person
      parties:                          # label and aliases as the source writes them
        - {party: cdu, label: CDU, aliases: [...]}
      extra_members:                    # persons whose item lacks the P39 statement
        - {id: Q50943765, label: Kristina Herbst}

A party names its Wikidata item by ``party`` (a key of ``GERMAN_PARTIES``, so each item is typed in exactly once) or, for a party not in that table, by ``id``.

Person coverage is as good as Wikidata's P39 statements: members whose item lacks the statement, and government members who hold no seat, stay unlinked.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

# Wikidata items of the parties sitting in German parliaments, by the key a
# manifest's ``parties[].party`` names.
GERMAN_PARTIES = {
    "afd": "Q6721203",            # Alternative für Deutschland
    "bsw": "Q123121346",          # Bündnis Sahra Wagenknecht
    "cdu": "Q49762",              # Christlich Demokratische Union Deutschlands
    "csu": "Q49763",              # Christlich-Soziale Union in Bayern
    "fdp": "Q13124",              # Freie Demokratische Partei
    "freie_waehler": "Q1231839",  # Landesvereinigung Freie Wähler Bayern
    "gruene": "Q49766",           # Bündnis 90/Die Grünen
    "linke": "Q49764",            # Die Linke
    "spd": "Q49768",              # Sozialdemokratische Partei Deutschlands
    "ssw": "Q161545",             # Südschleswigscher Wählerverband
}

_P39_MEMBERS = """
SELECT DISTINCT ?person ?label_de ?label_en ?label_mul WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:%(position)s .
  OPTIONAL { ?person rdfs:label ?label_de  FILTER(LANG(?label_de)="de") }
  OPTIONAL { ?person rdfs:label ?label_en  FILTER(LANG(?label_en)="en") }
  OPTIONAL { ?person rdfs:label ?label_mul FILTER(LANG(?label_mul)="mul") }
}
"""

_P39_MEMBERS_WITH_ALIASES = """
SELECT ?person ?label_de ?label_en ?label_mul
       (GROUP_CONCAT(DISTINCT ?alias_de; separator="|") AS ?aliases_de) WHERE {
  ?person p:P39 ?stmt .
  ?stmt ps:P39 wd:%(position)s .
  OPTIONAL { ?person rdfs:label ?label_de  FILTER(LANG(?label_de)="de") }
  OPTIONAL { ?person rdfs:label ?label_en  FILTER(LANG(?label_en)="en") }
  OPTIONAL { ?person rdfs:label ?label_mul FILTER(LANG(?label_mul)="mul") }
  OPTIONAL { ?person skos:altLabel ?alias_de  FILTER(LANG(?alias_de) IN ("de", "mul")) }
}
GROUP BY ?person ?label_de ?label_en ?label_mul
"""


def sparql_get(query: str, *, timeout: float = 120.0) -> dict:
    """Run a SPARQL query against Wikidata; retry with backoff on transport errors."""
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


def bind(binding: dict, key: str) -> str | None:
    """Pull a scalar value out of a SPARQL JSON result binding."""
    v = binding.get(key)
    return v.get("value") if isinstance(v, dict) else None


def write_entity_dump(metadata_dir: Path, payload: dict) -> Path:
    """Write ``metadata/entities.json`` deterministically; return its path."""
    metadata_dir.mkdir(parents=True, exist_ok=True)
    out = metadata_dir / "entities.json"
    with open(out, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return out


def fetch_p39_members(position: str, *, with_aliases: bool = False,
                      extra_members: list[dict] = ()) -> list[dict]:
    """Person entities for everyone with a ``P39`` (position held) statement for ``position``.

    The label is the German label, else the English one, else the language-independent (``mul``) one, else the QID; the other label variants become ``labelAlternative``. With ``with_aliases`` the German and ``mul`` aliases are added too, which covers name changes; single-word aliases (bare surnames) are left out so they never become a person key. ``extra_members`` (``{id, label}``) are appended unless the query already returned them.
    """
    logger.info(f"Querying Wikidata for holders of position {position}…")
    query = (_P39_MEMBERS_WITH_ALIASES if with_aliases else _P39_MEMBERS) % {"position": position}
    bindings = (sparql_get(query).get("results") or {}).get("bindings") or []
    members: dict[str, dict] = {}
    for b in bindings:
        iri = bind(b, "person") or ""
        if not iri:
            continue
        qid = iri.rsplit("/", 1)[-1]
        de = (bind(b, "label_de") or "").strip()
        en = (bind(b, "label_en") or "").strip()
        mul = (bind(b, "label_mul") or "").strip()
        label = de or en or mul or qid
        if with_aliases:
            aliases = [a.strip() for a in (bind(b, "aliases_de") or "").split("|")]
            alts = sorted({s for s in [en, mul, *aliases] if len(s.split()) > 1} - {label})
        else:
            alts = sorted({en, mul} - {label, ""})
        members[qid] = {
            "id": qid,
            "label": label,
            "subType": "memberOfParliament",
            "labelAlternative": alts,
        }
    logger.info(f"Got {len(members)} MPs from Wikidata")
    for extra in extra_members:
        members.setdefault(extra["id"], {**extra, "subType": "memberOfParliament",
                                         "labelAlternative": []})
    return list(members.values())


def build_p39_dump(metadata_dir: Path, *, position: str, parties: list[dict],
                   with_aliases: bool = False, extra_members: list[dict] = ()) -> Path:
    """Write ``entities.json`` from the holders of ``position`` plus ``parties`` (``{id, label, aliases}``)."""
    persons = fetch_p39_members(position, with_aliases=with_aliases, extra_members=extra_members)
    factions = [
        {
            "id": p["id"],
            "label": p["label"],
            "subType": "faction",
            "labelAlternative": list(p.get("aliases") or []),
        }
        for p in parties
    ]
    out = write_entity_dump(Path(metadata_dir), {"data": persons + factions})
    logger.info(f"Wrote {out} ({len(persons)} persons, {len(factions)} factions)")
    return out


def build_from_manifest(parliament_id: str, metadata_dir: Path) -> Path:
    """Build the dump the parliament's manifest describes under ``entity_bootstrap``."""
    from optv.parliaments import load_manifest
    config = load_manifest(parliament_id).get("entity_bootstrap")
    if not config:
        raise KeyError(f"The {parliament_id} manifest has no 'entity_bootstrap' block")
    parties = [{**party, "id": party["id"] if "id" in party else GERMAN_PARTIES[party["party"]]}
               for party in config.get("parties") or []]
    return build_p39_dump(
        metadata_dir,
        position=config["wikidata_position"],
        parties=parties,
        with_aliases=bool(config.get("wikidata_aliases")),
        extra_members=config.get("extra_members") or [],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build metadata/entities.json from Wikidata for a parliament "
                                                 "whose manifest has an entity_bootstrap block.")
    parser.add_argument("parliament", help="Parliament ID, e.g. DE-BW")
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    build_from_manifest(args.parliament, args.data_dir / "metadata")


if __name__ == "__main__":
    main()
