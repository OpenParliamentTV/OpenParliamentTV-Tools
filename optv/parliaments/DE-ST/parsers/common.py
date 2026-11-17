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
    "bsw": "BSW",
    "fraktionslos": "fraktionslos",
}

# Office keywords that mark a parenthesis in a transcript speaker line as the
# speaker's function (``Sven Schulze (Ministerpräsident):``) rather than a
# constituency (``Matthias Büttner (Staßfurt) (AfD):``).
_OFFICE_RE = re.compile(
    r"minister|präsident|berichterstatt|staatssekretär|beauftragt|vorsitzend",
    re.IGNORECASE)


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


def known_faction(label: str | None) -> str | None:
    """Canonical faction label if ``label`` is a known faction surface form."""
    if not label:
        return None
    return PARTY_NORMALISATION.get(normalize_ws(label).lower())


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


_NAME_PARTICLES = {"von", "van", "vom", "zu", "zur", "de", "der", "den", "di", "da"}


def looks_like_name(text: str) -> bool:
    """True for ``Dr. Hans-Thomas Tillschneider`` / ``Eva von Angern``.

    At least two tokens, each capitalised (honorifics included) or a name
    particle; prose such as ``Ich zitiere`` fails on the lower-case verb.
    """
    tokens = normalize_ws(text).split(" ")
    if len(tokens) < 2 or not tokens[0][:1].isupper():
        return False
    return all(t[:1].isupper() or t in _NAME_PARTICLES for t in tokens)


def name_key(name: str | None) -> str:
    """Comparable form of a person's name (honorifics dropped, lower case)."""
    return strip_honorifics(name or "").lower()


def parse_speaker_label(h3_text: str, has_profile: bool = False) -> dict:
    """Parse the ``<h3 class="no-style">`` content into speaker components.

    The portal renders one of these shapes:
      - ``Sven Schulze (CDU)  - Ministerpräsident``  → MP with party + role
      - ``Dr. Gunnar Schellenberger   - Landtagspräsident``  → presidium (no party)
      - ``Ulrich Siegmund (AfD)``  → member (the slot links the member's profile)
      - ``Eröffnung`` / ``Abstimmung (Namensaufruf)`` / ``Erster Wahlgang``
        → procedural marker, not a name

    A parenthesis counts as the faction only when it is a known faction or
    the slot links a member profile (``has_profile``); otherwise it is a
    qualifier of a procedural marker. A label is a person when it has a
    faction, a profile link, or a role after a name.

    Returns ``{"label", "party", "role", "is_procedural"}``. For a procedural
    marker ``label`` is empty; the merger takes the speaker from the
    transcript's first speaker line.
    """
    text = normalize_ws(h3_text or "")
    if not text:
        return {"label": "", "party": None, "role": None, "is_procedural": True}

    # Split off role at " - " (separator inside the label between name and function)
    role = None
    name_part = text
    if " - " in text:
        name_part, role_text = text.rsplit(" - ", 1)
        role = normalize_ws(role_text) or None

    parens = [normalize_ws(p) for p in _PAREN_RE.findall(name_part)]
    name = normalize_ws(_PAREN_RE.sub(" ", name_part))
    party = None
    for p in parens:
        party = known_faction(p) or party
    if party is None and has_profile and parens:
        party = normalize_party(parens[-1])

    is_person = bool(party or has_profile or (role and looks_like_name(name)))
    if not is_person:
        return {"label": "", "party": None, "role": None, "is_procedural": True}
    return {"label": name, "party": party, "role": role, "is_procedural": False}


_PRESIDIUM_LINE_RE = re.compile(
    r"^(?P<role>(?:Alters|Vize)?[Pp]räsident(?:in)?)\s+(?P<name>[^():]{3,80}?)\s*:$")
_PAREN_LINE_RE = re.compile(
    r"^(?P<name>[^():]{3,80}?)\s*(?P<parens>(?:\([^()]{1,160}\)\s*)+):$")


def parse_speaker_line(paragraph: str) -> dict | None:
    """Parse a transcript speaker line, or return ``None`` for any other text.

    The transcript (the Stenografischer Bericht text of one video clip) starts
    each contribution with a line of one of these shapes:
      - ``Präsident Tobias Rausch:`` / ``Vizepräsidentin Anne-Marie Keding:``
      - ``Daniel Roi (AfD):`` / ``Matthias Büttner (Staßfurt) (AfD):``
      - ``Sven Schulze (Ministerpräsident):`` / ``Gordon Köhler (Berichterstatter):``

    Returns ``{"name", "role", "faction", "presidium"}``. In a parenthesised
    line the known faction is the faction, a parenthesis naming an office is
    the role, and any other parenthesis (a constituency) is dropped.
    """
    text = normalize_ws(paragraph)
    if not text.endswith(":") or len(text) > 240:
        return None
    m = _PRESIDIUM_LINE_RE.match(text)
    if m and looks_like_name(m.group("name")):
        return {"name": normalize_ws(m.group("name")), "role": m.group("role"),
                "faction": None, "presidium": True}
    m = _PAREN_LINE_RE.match(text)
    if not m or not looks_like_name(m.group("name")):
        return None
    faction = role = None
    for p in (normalize_ws(x) for x in _PAREN_RE.findall(m.group("parens"))):
        if known_faction(p):
            faction = known_faction(p)
        elif role is None and _OFFICE_RE.search(p):
            role = p
    return {"name": normalize_ws(m.group("name")), "role": role,
            "faction": faction, "presidium": False}
