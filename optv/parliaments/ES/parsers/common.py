#! /usr/bin/env python3

# ES-specific normalisation helpers shared by media2json and proceedings2json.
#
# Two speaker spellings exist in the source data:
#   - interventions JSON (ORADOR): "Lastname, Firstname (GROUP)" — full name,
#     party present only ~77% of the time.
#   - HTML Diario: "El señor/La señora SURNAME (Role):" — uppercase surname only.
# The merger reconciles the two by surname; these helpers produce a canonical
# label ("Firstname Lastname") plus a separate surname for matching.

import logging
logger = logging.getLogger(__name__)

import re

# Parliamentary group abbreviations used in ORADOR / the Diario, XV legislature.
# Expanded to the official group names; unknown codes pass through unchanged.
FACTION_LABELS = {
    "GP": "Grupo Parlamentario Popular en el Congreso",
    "GS": "Grupo Parlamentario Socialista",
    "GSUMAR": "Grupo Parlamentario Plurinacional SUMAR",
    "GR": "Grupo Parlamentario Republicano",
    "GV": "Grupo Parlamentario Vasco (EAJ-PNV)",
    "GV (EAJ-PNV)": "Grupo Parlamentario Vasco (EAJ-PNV)",
    "GEH Bildu": "Grupo Parlamentario Euskal Herria Bildu",
    "GEHB": "Grupo Parlamentario Euskal Herria Bildu",
    "GMx": "Grupo Parlamentario Mixto",
    "GPlu": "Grupo Parlamentario Plural",
    "GVOX": "Grupo Parlamentario VOX",
    "GJxCAT": "Grupo Parlamentario Junts per Catalunya",
    "JxCAT": "Grupo Parlamentario Junts per Catalunya",
    "Cs": "Ciudadanos",
}


def fix_faction(abbr: str) -> str:
    """Expand a parliamentary-group abbreviation to its full label."""
    if not abbr:
        return abbr
    abbr = abbr.strip()
    return FACTION_LABELS.get(abbr, abbr)


# CARGOORADOR → (person type, whether the cargo itself is a meaningful role).
# Diputado/a is the default MP capacity (no extra role); ministers and the
# government presidency are members of government; chair roles stay MPs but
# carry the cargo as a role.
def classify_cargo(cargo: str):
    """Return (type, role) for a CARGOORADOR string."""
    c = (cargo or "").strip()
    cl = c.lower()
    if not c:
        return ("person", None)
    if cl.startswith("diputad"):
        return ("memberOfParliament", None)
    if cl.startswith("senador") or cl.startswith("senadora"):
        return ("memberOfParliament", c)
    if ("ministr" in cl
            or "presidente del gobierno" in cl
            or "vicepresident" in cl and "gobierno" in cl):
        return ("member of government", c)
    return ("person", c)


# Greedy so a group that itself contains parentheses — "GV (EAJ-PNV)" — is
# captured whole rather than stopping at the inner ")".
_PARTY_SUFFIX_RE = re.compile(r'\s*\((.+)\)\s*$')


def parse_orador(orador: str, cargo: str = "") -> dict:
    """Parse an ORADOR value into a person dict.

    "Gamarra Ruiz-Clavijo, Concepción (GP)" ->
        {label: "Concepción Gamarra Ruiz-Clavijo",
         firstname: "Concepción", lastname: "Gamarra Ruiz-Clavijo",
         context: "main-speaker", type: "memberOfParliament",
         faction: {label: "...Popular..."}}

    The merger matches media and proceedings speakers on `lastname` (the
    Diario gives only an uppercase surname), so it is always populated.
    """
    person: dict = {"context": "main-speaker"}
    name = (orador or "").strip()
    party = None
    m = _PARTY_SUFFIX_RE.search(name)
    if m:
        party = m.group(1).strip()
        name = name[:m.start()].strip()

    if "," in name:
        last, first = name.split(",", 1)
        lastname = last.strip()
        firstname = first.strip()
        label = f"{firstname} {lastname}".strip()
    else:
        lastname = name
        firstname = ""
        label = name

    person["label"] = label
    if lastname:
        person["lastname"] = lastname
    if firstname:
        person["firstname"] = firstname

    ptype, role = classify_cargo(cargo)
    if ptype:
        person["type"] = ptype
    if role:
        person["role"] = role
    if party:
        person["faction"] = {"label": fix_faction(party)}
    return person
