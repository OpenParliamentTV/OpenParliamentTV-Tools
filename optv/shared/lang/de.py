#! /usr/bin/env python3
"""German parliamentary-text conventions.

Shared by every German parliament (DE, DE-RP, DE-ST and the video-only
Landtage): academic-honorific stripping and chair-title → speaker ``context``
mapping. Add per-parliament deltas in that parliament's own merger/parser;
anything common to *German* belongs here.

The lower half of the module (``# --- PDF→TEI German strings ---``) holds the
German-language constants the generic PDF→TEI core in ``optv/shared/pdf2tei``
needs: month names, incident/result keywords, running-header and table-of-
contents heuristics, and the faction display/slug tables. The PDF core is
parliament-agnostic; the German knowledge it consumes lives here.
"""

from __future__ import annotations

import re
import unicodedata

# German academic honorifics stripped from speaker names before first/last split.
HONORIFICS = ("Dr. ", "Prof. ", "Prof. Dr. ", "Dr. Dr. ", "Dr. h. c. ")


def strip_honorifics(name: str, honorifics: tuple[str, ...] = HONORIFICS) -> str:
    """Repeatedly strip leading academic honorifics (``Dr. ``, ``Prof. ``…)."""
    s = name.strip()
    changed = True
    while changed:
        changed = False
        for h in honorifics:
            if s.startswith(h):
                s = s[len(h):]
                changed = True
                break
    return s


def speaker_context(role: str) -> str:
    """Map a German chair/role string to a Stage-2 speaker ``context``.

    Vice-president variants (``Vizepräsident``, ``stellv. Präsident``) →
    ``vice-president``; president variants → ``president``; everything else
    (MPs, government members) → ``main-speaker``.
    """
    r = (role or "").lower()
    if ("vizepräsident" in r or "vizepraesident" in r
            or ("stellv" in r and ("präsident" in r or "praesident" in r))):
        return "vice-president"
    if "präsident" in r or "praesident" in r:
        return "president"
    return "main-speaker"


# --------------------------------------------------------------------------- #
# --- PDF→TEI German strings (consumed by optv/shared/pdf2tei) ---
# --------------------------------------------------------------------------- #

# Month name -> number, for parsing the sitting date out of the protocol head.
MONTHS = {m: i for i, m in enumerate(
    ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August",
     "September", "Oktober", "November", "Dezember"], start=1)}

DATE_RE = re.compile(r"(\d{1,2})\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|"
                     r"August|September|Oktober|November|Dezember)\s+(\d{4})")

# Interjection / reaction keywords ("(Beifall bei der SPD)", "(Zwischenruf …)").
INCIDENT_KW = re.compile(r"Beifall|Zuruf|Zwischenruf|Heiterkeit|Unruhe|Lachen|"
                         r"Widerspruch|Vereinzelt", re.IGNORECASE)

# Blocks that mark the end of an agenda *title* (procedural metadata follows).
TITLE_STOP = re.compile(r"^\s*(–|-|dazu\b|Drucksache\b|Antr[äa]ge?\b|Antrag\b|"
                        r"Beschlussempfehlung\b|Wahlvorschlag\b|Bericht\b|"
                        r"Gesetzentwurf\b|Unterrichtung\b|Entschließung)", re.IGNORECASE)

# Stray headers that look title-ish but are not agenda items.
HEADING_NOISE = re.compile(r"^(Regierungsbank|Beginn|Schluss|Unterbrechung|"
                           r"Fortsetzung|Wiederbeginn|Präsidium|Anwesend)\b", re.IGNORECASE)

# Chair role keyword -> ParlaMint speaker context.
CHAIR_ROLE_CONTEXT = {
    "präsident": "president",
    "präsidentin": "president",
    "vizepräsident": "vice-president",
    "vizepräsidentin": "vice-president",
    "alterspräsident": "interim-president",
    "alterspräsidentin": "interim-president",
}


def pdf_chair_context(role_text: str) -> str:
    """Map any chair role label to a speaker context, handling ordinals
    ('Erster Vizepräsident', 'Präsidentin', 'Alterspräsident', …)."""
    r = role_text.lower()
    if "alterspräsident" in r:
        return "interim-president"
    if "vizepräsident" in r:
        return "vice-president"
    if "präsident" in r:
        return "president"
    return CHAIR_ROLE_CONTEXT.get(r, "speaker")


def is_running_header(text: str) -> bool:
    """Match the page running header / footer of a DE Landtag protocol (or an
    empty block) — lines the PDF text layer repeats per page and the parser
    must drop."""
    t = text.strip()
    if not t:
        return True
    if re.match(r"^LANDTAG\b", t) or ("Wahlperiode" in t and "Plenarsitzung" in t):
        return True
    if re.match(r"^Schleswig-Holsteinischer Landtag", t):
        return True
    if re.fullmatch(r"\d{1,4}", t):          # bare page number
        return True
    return False


# Table-of-contents heuristics (front-matter agenda listing).
TOC_HEADER_LINE = re.compile(
    r"^(LANDTAG\b|Bayerischer Landtag\b|\d+\.\s+Wahlperiode\b|"
    r"\d+\.\s+Plenarsitzung\b)|Plenarprotokoll", re.IGNORECASE)
_FACTION_IN_REF = r"(?:SPD|CDU|CSU|FDP|SSW|AfD|BÜNDNIS|DIE LINKE|GRÜNE|FREIE WÄHLER|fraktionslos)"
TOC_SPEAKER_REF = re.compile(
    r"\bAbg\.|,\s*\w*(minister(?:in)?|pr[äa]sident(?:in)?|sekret[äa]r(?:in)?|"
    r"senator(?:in)?|b[üu]rgermeister(?:in)?|staatsr[äa]t(?:in)?)\b"
    r"|^(Staatsminister(?:in)?|Staatssekret[äa]r(?:in)?|Ministerpr[äa]sident(?:in)?)\b"
    rf"|[\[(]\s*{_FACTION_IN_REF}\s*[\])]", re.IGNORECASE)
TOC_RESULT_LINE = re.compile(r"^(\d+\.\s+(Annahme|Ablehnung|Überweisung|Fassung)\b"
                             r"|\(neu\)|Beschluss\b)", re.IGNORECASE)

# Faction display labels (longest-first for matching) and xml:id slugs.
FACTIONS = [
    "BÜNDNIS 90/DIE GRÜNEN", "DIE LINKE", "FREIE WÄHLER",
    "CDU", "SPD", "AfD", "FDP", "SSW", "Die Linke", "GRÜNE",
]
FACTION_SLUG = {
    "BÜNDNIS 90/DIE GRÜNEN": "GRUENE",
    "GRÜNE": "GRUENE",
    "DIE LINKE": "LINKE",
    "Die Linke": "LINKE",
    "FREIE WÄHLER": "FW",
    "CDU": "CDU", "SPD": "SPD", "AfD": "AfD", "FDP": "FDP", "SSW": "SSW",
}


def faction_slug(label: str) -> str:
    """Faction display label -> NCName-safe xml:id slug."""
    return FACTION_SLUG.get(label.strip(),
                            re.sub(r"[^A-Za-z0-9]+", "_", label.strip()).strip("_") or "X")


# Leading role tokens to drop before taking a surname ("Präsidentin Ilse Aigner"
# -> "Ilse Aigner"; "Staatsminister Alexander Schweitzer" -> "Alexander …").
_LEADING_ROLE = re.compile(
    r"^(?:Abg\.\s+|(?:Erste[rn]?|Zweite[rn]?|Dritte[rn]?|Vierte[rn]?|F[üu]nfte[rn]?|"
    r"Sechste[rn]?|Siebte[rn]?)\s+)?"
    r"(?:Landtags|Bundes)?"
    r"(?:Vize)?Pr[äa]sident(?:in)?|Alterspr[äa]sident(?:in)?|"
    r"Ministerpr[äa]sident(?:in)?|Staatsminister(?:in)?|Staatssekret[äa]r(?:in)?|"
    r"Staatsr[äa]t(?:in)?|Senator(?:in)?|Minister(?:in)?|B[üu]rgermeister(?:in)?",
    re.IGNORECASE)


def match_key_surname(name: str) -> str:
    """Normalised surname match key for the spine⋈proceedings join.

    Strips honorifics and a leading chair/government role, drops a trailing
    faction parenthetical / ", FAKTION", then returns the accent-folded,
    lower-cased **last** name token. Same key shape on both the media spine and
    the proceedings text turn (German chairs carry a name in both)."""
    s = strip_honorifics((name or "").strip())
    s = re.sub(r"\s*[\[(].*?[\])]\s*$", "", s)       # trailing "(SPD)" / "[CDU]"
    s = re.sub(r",\s*[^,]+$", "", s) if "," in s else s   # trailing ", SPD"
    m = _LEADING_ROLE.match(s)
    if m:
        s = s[m.end():].strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    toks = re.sub(r"[^0-9A-Za-z \-]", " ", s).split()
    return toks[-1].lower() if toks else ""
