"""Shared helpers for EU verbatim parsers (speaker name & faction normalization).

The EU pipeline pulls speech text from the EP Open Data Portal API (English
translations only), so per-speech language detection and the multilingual
"on behalf of" phrasing list that the HTML scraper used to need are gone.
``parse_speaker_line`` is kept as a fallback for the cases where the API's
structured ``<person>`` / ``<organization>`` elements are missing.
"""

from __future__ import annotations

import re

# EU political group abbreviations as they appear in CRE markup, mapped to a
# canonical "wtype: faction" label. Source: EP procedural rules + Wikipedia
# political-group articles for term 10 (2024-2029).
EU_FACTION_LABELS = {
    "PPE":       "European People's Party",
    "S&D":       "Progressive Alliance of Socialists and Democrats",
    "Renew":     "Renew Europe",
    "Verts/ALE": "The Greens / European Free Alliance",
    "ECR":       "European Conservatives and Reformists",
    "ID":        "Identity and Democracy",       # term 9 — gone in term 10
    "The Left":  "The Left in the European Parliament - GUE/NGL",
    "PfE":       "Patriots for Europe",          # term 10
    "ESN":       "Europe of Sovereign Nations",  # term 10
    "NI":        "Non-attached Members",
}

# Roles that appear before the dash in the speaker line for non-MEPs.
EU_KNOWN_ROLES = {
    "President",
    "Vice-President",
    "Acting President",
    "Rapporteur",
    "Commissioner",
    "Member of the Commission",
    "Vice-President of the Commission",
    "Executive Vice-President of the Commission",
    "President of the Commission",
    "President-in-Office of the Council",
    "President of the Council",
    "Member of the Council",
}

_PAREN_RE = re.compile(r"\(([^)]+)\)")
# Last-token faction match for English "on behalf of <Group>" phrasing.
_FACTION_TAIL_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in sorted(EU_FACTION_LABELS, key=len, reverse=True)) +
    r")\b(?:\s+(?:Group|group))?\s*$"
)
_ON_BEHALF_OF_RE = re.compile(r",?\s*on behalf of(?: the)?\b\s*", re.IGNORECASE)


def parse_speaker_line(text: str) -> dict:
    """Parse a free-text speaker line into name/role/faction parts.

    Used as a fallback when the API's structured ``<person>`` /
    ``<organization>`` elements aren't usable. Returns::

        {"name": str, "role": str | None, "factionAbbr": str | None,
         "factionLabel": str | None, "annotation": str | None}
    """
    raw = text.strip()
    if raw.endswith("."):
        raw = raw[:-1].strip()

    faction_abbr = None
    annotation: str | None = None

    paren_m = _PAREN_RE.search(raw)
    if paren_m:
        candidate = paren_m.group(1).strip()
        if candidate in EU_FACTION_LABELS:
            faction_abbr = candidate
            head = raw[:paren_m.start()].rstrip(" ,")
            tail = raw[paren_m.end():].lstrip(" ,").strip()
            if tail:
                annotation = tail
            raw = head

    if faction_abbr is None:
        tail_m = _FACTION_TAIL_RE.search(raw)
        if tail_m:
            faction_abbr = tail_m.group(1)
            raw = raw[:tail_m.start()].rstrip(" ,")
            raw = _ON_BEHALF_OF_RE.sub("", raw).rstrip(" ,").strip()

    role = None
    if "," in raw:
        name_part, _, role_candidate = raw.rpartition(",")
        role_candidate = role_candidate.strip()
        if role_candidate in EU_KNOWN_ROLES:
            role = role_candidate
            raw = name_part.strip()

    name = raw
    if not role and name in EU_KNOWN_ROLES:
        role = name

    return {
        "name": name,
        "role": role,
        "factionAbbr": faction_abbr,
        "factionLabel": EU_FACTION_LABELS.get(faction_abbr) if faction_abbr else None,
        "annotation": annotation,
    }
