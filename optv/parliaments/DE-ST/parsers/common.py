"""Shared parser helpers for DE-ST."""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


# Roles in the speaker label come after " - " (e.g. "- Landtagspräsident",
# "- Fraktionsvorsitzende", "- Ministerpräsident"). We translate the
# presidium roles to the Stage 2 ``context`` enum used by other parliaments;
# substantive ministerial roles stay free-text on ``role``.
ROLE_TO_CONTEXT: dict[str, str] = {
    "landtagspräsident": "president",
    "landtagspräsidentin": "president",
    "präsident": "president",
    "präsidentin": "president",
    "vizepräsident": "vice-president",
    "vizepräsidentin": "vice-president",
    "alterspräsident": "interim-president",
    "alterspräsidentin": "interim-president",
}


# Surface-form variants the source HTML emits for each party, normalised to
# the canonical label used by the entity dump / NEL stage.
PARTY_NORMALISATION: dict[str, str] = {
    "die linke": "Die Linke",
    "die linke.": "Die Linke",
    "linke": "Die Linke",
    "bündnis 90/die grünen": "BÜNDNIS 90/DIE GRÜNEN",
    "bündnis90/die grünen": "BÜNDNIS 90/DIE GRÜNEN",
    "grüne": "BÜNDNIS 90/DIE GRÜNEN",
    "grünen": "BÜNDNIS 90/DIE GRÜNEN",
    "cdu": "CDU",
    "spd": "SPD",
    "fdp": "FDP",
    "afd": "AfD",
}


_WS_RE = re.compile(r"\s+")
_PAREN_RE = re.compile(r"\s*\(([^)]+)\)\s*")


def normalize_ws(s: str) -> str:
    if s is None:
        return ""
    return _WS_RE.sub(" ", s.replace("\xa0", " ")).strip()


def normalize_party(label: str | None) -> str | None:
    if not label:
        return None
    key = normalize_ws(label).lower()
    return PARTY_NORMALISATION.get(key, normalize_ws(label))


def role_to_context(role: str | None) -> str | None:
    if not role:
        return None
    return ROLE_TO_CONTEXT.get(role.strip().lower())


# Common honorifics to strip when computing the "bare" name for label dedup.
_HONORIFICS = ("Dr. ", "Prof. ", "Prof. Dr. ", "Dr. Dr. ", "Dr. h. c. ")


def strip_honorifics(name: str) -> str:
    s = normalize_ws(name)
    changed = True
    while changed:
        changed = False
        for h in _HONORIFICS:
            if s.startswith(h):
                s = s[len(h):]
                changed = True
                break
    return s


def parse_speaker_label(h3_text: str) -> dict:
    """Parse the ``<h3 class="no-style">`` content into speaker components.

    The portal renders one of these shapes:
      - ``Sven Schulze (CDU)  - Ministerpräsident``  → MP with party + role
      - ``Dr. Gunnar Schellenberger   - Landtagspräsident``  → presidium (no party)
      - ``Eröffnung`` / ``Wahl`` / ``Vereidigung``  → procedural marker, not a name

    Returns ``{"label", "party", "role", "is_procedural"}``. When the h3 has
    no name structure (procedural marker), ``label`` is left empty and the
    merger fills it in from the transcript's speaker prefix line.
    """
    text = normalize_ws(h3_text or "")
    if not text:
        return {"label": "", "party": None, "role": None, "is_procedural": True}

    # Split off role at " - " (separator inside the label between name and function)
    role = None
    name_part = text
    if " - " in text:
        name_part, role_text = text.rsplit(" - ", 1)
        name_part = normalize_ws(name_part)
        role = normalize_ws(role_text)

    # Extract party from parens
    party = None
    m = _PAREN_RE.search(name_part)
    if m:
        party = normalize_party(m.group(1))
        name_part = _PAREN_RE.sub(" ", name_part)
        name_part = normalize_ws(name_part)

    # If the remaining "name" lacks any lowercase letter and no role/party
    # was found, treat as procedural (Wahl, Vereidigung, Eröffnung, ...).
    is_procedural = False
    if not party and not role:
        # All caps / short noun → procedural-only
        if name_part and not re.search(r"\b[A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+", name_part):
            is_procedural = True
            name_part = ""

    return {
        "label": name_part,
        "party": party,
        "role": role,
        "is_procedural": is_procedural,
    }
