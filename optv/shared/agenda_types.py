"""Agenda-item type classification.

Two-tier model:

- ``nativeType`` is the parliament-specific identifier. For DE we adopt the
  ParlaMint-DE vocabulary verbatim (e.g. ``DE-question_time``,
  ``DE-current_affairs``) so OPTV outputs interop with PolMine / ParlaMint
  corpora. For SE the native value is Riksdag's ``kammaraktivitet`` string.

- ``type`` is a small cross-parliament enum that consumers (search, frontend
  filtering, the QC tool) can rely on without learning each parliament's
  vocabulary.

Each parser calls the matching ``classify_*`` helper and copies both fields
onto the agendaItem dict. ``nativeType`` is optional; ``type`` falls back to
``CORE_REGULAR`` when nothing more specific is detectable.
"""

from __future__ import annotations

import re
from typing import Optional


# ---------------------------------------------------------------------------
# Cross-parliament core enum
# ---------------------------------------------------------------------------

CORE_REGULAR = "regular"                            # plenary debate, motion, consultation
CORE_QA = "qa"                                      # Fragestunde / Q&A
CORE_GOVERNMENT_QUESTIONING = "questioning_of_the_government"  # Befragung der Bundesregierung
CORE_CURRENT_AFFAIRS = "current_affairs"            # Aktuelle Stunde
CORE_GOVERNMENT_DECLARATION = "government_declaration"
CORE_BUDGET = "budget"
CORE_ELECTION = "election"                          # Wahl
CORE_VOTING = "voting"
CORE_OATH = "oath"
CORE_BRIEFING = "briefing"
CORE_REPORT = "report"
CORE_RECOMMENDATION = "recommendation"
CORE_OPENING = "opening"
CORE_CLOSING = "closing"
CORE_CONDOLENCE = "condolence"
CORE_RULES_OF_PROCEDURE = "rules_of_procedure"
# Chair-only inter-TOP transition turn ("Ich schließe die Aussprache,
# ich rufe Tagesordnungspunkt N auf, ..."). Not substantive speech; used by
# the merger to gate-fail framing turns whose proceedings text does not
# match the audio of any single media clip.
CORE_PROCEDURAL = "procedural"
CORE_OTHER = "other"

CORE_TYPES = frozenset({
    CORE_REGULAR, CORE_QA, CORE_GOVERNMENT_QUESTIONING, CORE_CURRENT_AFFAIRS,
    CORE_GOVERNMENT_DECLARATION, CORE_BUDGET, CORE_ELECTION, CORE_VOTING,
    CORE_OATH, CORE_BRIEFING, CORE_REPORT, CORE_RECOMMENDATION, CORE_OPENING,
    CORE_CLOSING, CORE_CONDOLENCE, CORE_RULES_OF_PROCEDURE, CORE_PROCEDURAL,
    CORE_OTHER,
})


def _match_title(value, patterns):
    """Shared first-match dispatch for the regex-on-a-single-string
    classifiers. Returns ``(native_type, core_type)`` from the first
    pattern whose ``search`` hits ``value``; ``(None, CORE_REGULAR)``
    when ``value`` is empty or nothing matches. New parliaments only need
    a ``_X_PATTERNS`` list + a one-line wrapper (or call this directly) —
    no bespoke loop.
    """
    if not value:
        return None, CORE_REGULAR
    for pat, native, core in patterns:
        if pat.search(value):
            return native, core
    return None, CORE_REGULAR


# ---------------------------------------------------------------------------
# DE — ParlaMint vocabulary (period 17, structured `ana` attribute)
# ---------------------------------------------------------------------------

# Strongest signal first. The ana attribute on a debateSection often carries
# multiple tokens (e.g. "#DE-motion #DE-consultation #DE-report"); we pick the
# most specific procedural class. Tokens not listed fall through to "regular".
PARLAMINT_DE_TOKEN_PRIORITY: list[tuple[str, str]] = [
    ("DE-question_time", CORE_QA),
    ("DE-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    ("DE-current_affairs", CORE_CURRENT_AFFAIRS),
    ("DE-government_declaration", CORE_GOVERNMENT_DECLARATION),
    ("DE-election", CORE_ELECTION),
    ("DE-voting", CORE_VOTING),
    ("DE-oath", CORE_OATH),
    ("DE-sworn_in", CORE_OATH),
    ("DE-swearing_in", CORE_OATH),
    ("DE-condolence", CORE_CONDOLENCE),
    ("DE-rules_of_procedure", CORE_RULES_OF_PROCEDURE),
    ("DE-opening_speech", CORE_OPENING),
    ("DE-assumption", CORE_OPENING),
    ("DE-budget", CORE_BUDGET),
    ("DE-briefing", CORE_BRIEFING),
    ("DE-report", CORE_REPORT),
    ("DE-recommendation", CORE_RECOMMENDATION),
    ("DE-debate", CORE_REGULAR),
    ("DE-consultation", CORE_REGULAR),
    ("DE-motion", CORE_REGULAR),
    ("DE-misc", CORE_OTHER),
]


_DE_CLOSING_TEXT_RE = re.compile(
    r"\b(Sitzung\s+ist\s+geschlossen|schließe\s+die\s+Sitzung|"
    r"beende\s+die\s+Sitzung|hebe\s+die\s+Sitzung\s+auf|"
    r"Sitzung\s+ist\s+beendet)\b",
    re.I,
)


def is_de_closing_chair_text(text: Optional[str]) -> bool:
    """Heuristic: does this chair utterance close the sitting?

    Used by the ParlaMint parser, which has no structural ``ana`` token for
    closing — the closing chair turn lives inside whatever debateSection
    happens to be last. We override the inherited type when the text matches.
    """
    if not text:
        return False
    return bool(_DE_CLOSING_TEXT_RE.search(text))


def classify_parlamint_de(ana: Optional[str]) -> tuple[Optional[str], str]:
    """Classify a ParlaMint-DE debateSection by its `ana` attribute.

    Returns (native_type, core_type). native_type is the first DE-* token
    encountered (so ``"#DE-motion #DE-consultation"`` → ``"DE-motion"``);
    core_type is derived by priority over the full token set.
    """
    if not ana:
        return None, CORE_REGULAR
    tokens = {t.lstrip("#") for t in ana.split() if t.startswith("#DE-")}
    if not tokens:
        return None, CORE_REGULAR

    core = CORE_REGULAR
    for token, mapped in PARLAMINT_DE_TOKEN_PRIORITY:
        if token in tokens:
            core = mapped
            break

    # Native type = first DE-* token in document order, preserved verbatim.
    for raw in ana.split():
        bare = raw.lstrip("#")
        if bare.startswith("DE-"):
            return bare, core
    return None, core


# ---------------------------------------------------------------------------
# DE — Bundestag native (period 18+) — title regex
# ---------------------------------------------------------------------------

# Patterns scanned on agendaItem title / officialTitle. Order matters — the
# first match wins. Each pattern maps to (native_type, core_type) using the
# ParlaMint-DE vocabulary so periods 17 and 18+ produce comparable values.
# Patterns are anchored loosely (case-insensitive, allow surrounding text) so
# they work on titles like "Fragestunde — Drucksache 17/49".
_DE_NATIVE_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    # Closing first — "Sitzungsende" must beat the looser "Sitzungs-" / regular fallthrough.
    (re.compile(r"\b(Sitzungs(ende|schluss)|Schluss\s+der\s+Sitzung|Ende\s+der\s+Sitzung)\b", re.I),
     "DE-closing", CORE_CLOSING),
    (re.compile(r"\bBefragung\s+der\s+(?:Bundesregierung|BReg)\b", re.I),
     "DE-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b", re.I),
     "DE-question_time", CORE_QA),
    # ParlaMint per-question slots inside a Fragestunde, e.g.
    # "BMVg Frage 01", "BMU Frage 03", "BMELV Frage 02". Ministry codes start
    # with BM (Bundesministerium) and may have lower-case letters at the end
    # ("BMVg" = Verteidigung).
    (re.compile(r"\bBM[A-Za-z]{1,6}\s+Frage\s+\d+\b", re.I),
     "DE-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+Stunde\b", re.I),
     "DE-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(der|des)\b", re.I),
     "DE-election", CORE_ELECTION),
    (re.compile(r"\bAbstimmung\b", re.I),
     "DE-voting", CORE_VOTING),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid)\b", re.I),
     "DE-oath", CORE_OATH),
    (re.compile(r"\bAmts[üu]bernahme\b", re.I),
     "DE-assumption", CORE_OPENING),
    (re.compile(r"\b(Sitzungs)?Er[öo]ffnung(srede|\s+der\s+Sitzung)?\b", re.I),
     "DE-opening_speech", CORE_OPENING),
    (re.compile(r"\bGesch[äa]ftsordnung", re.I),
     "DE-rules_of_procedure", CORE_RULES_OF_PROCEDURE),
    (re.compile(r"\b(W[üu]rdigung|Gedenken)\b", re.I),
     "DE-condolence", CORE_CONDOLENCE),
    (re.compile(r"\bHaushalts(gesetz|plan)\b|\bEinzelplan\b", re.I),
     "DE-budget", CORE_BUDGET),
]


def classify_de_native(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Bundestag period-18+ data by agenda title regex."""
    return _match_title(title, _DE_NATIVE_PATTERNS)


# ---------------------------------------------------------------------------
# DE-RP — Landtag Rheinland-Pfalz title regex
# ---------------------------------------------------------------------------

# DE-RP titles are mostly substantive bill names ("Drittes Landesgesetz...",
# "Einzelplan 06 ..."). Few procedural markers appear in titles.
_DE_RP_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bFragestunde\b", re.I),
     "DE-RP-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+(?:Debatte|Stunde)\b", re.I),
     "DE-RP-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-RP-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(der|des)\b", re.I),
     "DE-RP-election", CORE_ELECTION),
    (re.compile(r"\bEinzelplan\b|\bLandeshaushaltsgesetz\b|\bFinanzplan\b", re.I),
     "DE-RP-budget", CORE_BUDGET),
    (re.compile(r"\b(Vereidigung|Eidesleistung)\b", re.I),
     "DE-RP-oath", CORE_OATH),
]


def classify_de_rp(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Landtag RLP agenda by title."""
    return _match_title(title, _DE_RP_PATTERNS)


# ---------------------------------------------------------------------------
# DE-ST — Landtag Sachsen-Anhalt title regex
# ---------------------------------------------------------------------------

# DE-ST publishes a per-TOP "Tagesordnungspunkt-Art" (Wahl, Vereidigung,
# Beratung, Erste Beratung, Zweite Beratung, Aktuelle Debatte, Fragestunde,
# Befragung der Landesregierung, Eröffnung, Abstimmung). It is rendered as a
# short prefix on the agenda title block, so we match it as a leading
# fragment when present and fall back to looser whole-title matches.
_DE_ST_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bBefragung\s+der\s+Landesregierung\b", re.I),
     "DE-ST-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b", re.I),
     "DE-ST-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+(?:Debatte|Stunde)\b", re.I),
     "DE-ST-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-ST-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\b", re.I),
     "DE-ST-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid)\b", re.I),
     "DE-ST-oath", CORE_OATH),
    (re.compile(r"\bEr[öo]ffnung\b", re.I),
     "DE-ST-opening", CORE_OPENING),
    (re.compile(r"\bAbstimmung\b", re.I),
     "DE-ST-voting", CORE_VOTING),
    (re.compile(r"\b(Erste|Zweite|Dritte)\s+Beratung\b", re.I),
     "DE-ST-beratung", CORE_REGULAR),
    (re.compile(r"\bBeratung\b", re.I),
     "DE-ST-beratung", CORE_REGULAR),
    (re.compile(r"\bHaushalts(gesetz|plan)\b|\bEinzelplan\b|\bFinanzplan\b", re.I),
     "DE-ST-budget", CORE_BUDGET),
]


def classify_de_st(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Landtag Sachsen-Anhalt agenda by TOP title."""
    return _match_title(title, _DE_ST_PATTERNS)


# ---------------------------------------------------------------------------
# DE-SH — Landtag Schleswig-Holstein title regex
# ---------------------------------------------------------------------------

# The m7k mediathek exposes a short ``thema`` per speech rather than a full
# Plenarprotokoll TOP title (e.g. "Eröffnung der Sitzung durch den
# Alterspräsidenten", "Wahl und Vereidigung der Landtagspräsidentin"). The
# regex set mirrors what shows up in practice; titles outside these procedural
# categories fall through to CORE_REGULAR.
_DE_SH_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bBefragung\s+der\s+Landesregierung\b", re.I),
     "DE-SH-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b", re.I),
     "DE-SH-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+(?:Debatte|Stunde)\b", re.I),
     "DE-SH-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-SH-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:der|des|und)\b", re.I),
     "DE-SH-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid)\b", re.I),
     "DE-SH-oath", CORE_OATH),
    (re.compile(r"\bEr[öo]ffnung\b", re.I),
     "DE-SH-opening", CORE_OPENING),
    (re.compile(r"\bHaushalts(gesetz|plan)\b|\bEinzelplan\b|\bFinanzplan\b", re.I),
     "DE-SH-budget", CORE_BUDGET),
]


def classify_de_sh(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Landtag Schleswig-Holstein agenda by m7k ``thema`` / TOP title."""
    return _match_title(title, _DE_SH_PATTERNS)


# ---------------------------------------------------------------------------
# DE-BY — Bayerischer Landtag "Plenum Online" TOP title regex
# ---------------------------------------------------------------------------

# The accordion header carries a full TOP title, e.g.
# "TOP 1a) Erste Lesung zum Gesetzentwurf …", "Aktuelle Stunde …",
# "Fragestunde", "Befragung der Staatsregierung". Titles outside these
# procedural categories fall through to CORE_REGULAR (most readings, motions,
# interpellations are substantive debate).
_DE_BY_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bBefragung\s+der\s+Staatsregierung\b", re.I),
     "DE-BY-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b|\bM[üu]ndliche\s+Anfragen\b", re.I),
     "DE-BY-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+Stunde\b", re.I),
     "DE-BY-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-BY-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:der|des|und|zum|zur)\b", re.I),
     "DE-BY-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid)\b", re.I),
     "DE-BY-oath", CORE_OATH),
    (re.compile(r"\bEr[öo]ffnung\b|\bkonstituierende\b", re.I),
     "DE-BY-opening", CORE_OPENING),
    (re.compile(r"\bHaushalts(gesetz|plan)\b|\bEinzelplan\b|\bNachtragshaushalt\b", re.I),
     "DE-BY-budget", CORE_BUDGET),
]


def classify_de_by(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Bayerischer Landtag agenda by the Plenum Online TOP title."""
    return _match_title(title, _DE_BY_PATTERNS)


# ---------------------------------------------------------------------------
# DE-BW — Landtag von Baden-Württemberg mediathek chapter-list TOP title regex
# ---------------------------------------------------------------------------

# The mediathek chapter list gives a TOP header ("TOP 1 Aktuelle Debatte",
# "TOP 3 Zweite Beratung", "Beginn der Sitzung") plus a substantive
# description. We classify the combined title+description; first match wins.
# Readings/motions/interpellations are substantive debate and fall through to
# CORE_REGULAR.
_DE_BW_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bRegierungsbefragung\b|\bBefragung\s+der\s+Landesregierung\b", re.I),
     "DE-BW-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b|\bM[üu]ndliche\s+Anfragen\b", re.I),
     "DE-BW-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+(?:Debatte|Stunde)\b", re.I),
     "DE-BW-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-BW-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:der|des|und|zum|zur)\b", re.I),
     "DE-BW-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid|Verpflichtung)\b", re.I),
     "DE-BW-oath", CORE_OATH),
    (re.compile(r"\bBeginn\s+der\s+Sitzung\b|\bEr[öo]ffnung\b|\bkonstituierende\b", re.I),
     "DE-BW-opening", CORE_OPENING),
    (re.compile(r"\bAbstimmung\b", re.I),
     "DE-BW-voting", CORE_VOTING),
    (re.compile(r"\bStaatshaushaltsplan\b|\bHaushalts(gesetz|plan)\b|\bEinzelplan\b|"
                r"\bNachtragshaushalt\b|\bStaatshaushalt\b", re.I),
     "DE-BW-budget", CORE_BUDGET),
]


def classify_de_bw(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Landtag Baden-Württemberg agenda by the mediathek TOP title."""
    return _match_title(title, _DE_BW_PATTERNS)


# ---------------------------------------------------------------------------
# DE-HH — Hamburgische Bürgerschaft mediathek agenda-item title regex
# ---------------------------------------------------------------------------

# The mediathek agenda-item header gives a TOP title ("AKTUELLE STUNDE …",
# "Aktuelle Befragung des Senats", a motion/bill title, or a memorial header).
# We classify the title; first match wins. Motions/bills/interpellations are
# substantive debate and fall through to CORE_REGULAR.
_DE_HH_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bAktuelle\s+(?:Stunde|Debatte)\b", re.I),
     "DE-HH-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\b(?:Aktuelle\s+)?Befragung\s+des\s+Senats\b|\bSenatsbefragung\b", re.I),
     "DE-HH-questioning_of_the_senate", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b|\bM[üu]ndliche\s+Anfragen\b", re.I),
     "DE-HH-question_time", CORE_QA),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-HH-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:der|des|und|zum|zur|von)\b", re.I),
     "DE-HH-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Verpflichtung|Amtseid|Eidesleistung)\b", re.I),
     "DE-HH-oath", CORE_OATH),
    (re.compile(r"\bGedenk(?:worte|en|minute)\b|\bNachruf\b|\bGedenkstunde\b", re.I),
     "DE-HH-condolence", CORE_CONDOLENCE),
    (re.compile(r"\bHaushalts(?:gesetz|plan)\b|\bHaushaltsplan\b|\bEinzelplan\b|"
                r"\bNachtragshaushalt\b|\bHaushalt\b", re.I),
     "DE-HH-budget", CORE_BUDGET),
]


def classify_de_hh(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Hamburgische Bürgerschaft agenda by the mediathek TOP title."""
    return _match_title(title, _DE_HH_PATTERNS)


# ---------------------------------------------------------------------------
# DE-NW — Landtag Nordrhein-Westfalen mediathek agenda-item (TOP) title regex
# ---------------------------------------------------------------------------

# The mediathek video page gives a per-TOP title in an ``<h3 class="e-top__title">``
# (e.g. "Sitzungseröffnung …", "Aktuelle Stunde …", "Fragestunde", a
# Gesetzentwurf / Antrag title, "in Verbindung damit" combined headers). We
# classify the title; first match wins. Bills/motions/interpellations are
# substantive debate and fall through to CORE_REGULAR.
_DE_NW_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bUnterrichtung\s+durch\s+die\s+Landesregierung\b|"
                r"\bBefragung\s+der\s+Landesregierung\b", re.I),
     "DE-NW-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b|\bM[üu]ndliche\s+Anfragen\b", re.I),
     "DE-NW-question_time", CORE_QA),
    (re.compile(r"\b(Große|Grosse)\s+Anfrage\b", re.I),
     "DE-NW-interpellation", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bAktuelle\s+Stunde\b", re.I),
     "DE-NW-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-NW-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:der|des|und|zum|zur|von)\b", re.I),
     "DE-NW-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid|Verpflichtung)\b", re.I),
     "DE-NW-oath", CORE_OATH),
    (re.compile(r"\bGedenk(?:en|worte|stunde)\b|\bNachruf\b|\bW[üu]rdigung\b", re.I),
     "DE-NW-condolence", CORE_CONDOLENCE),
    (re.compile(r"\b(Sitzungs)?Er[öo]ffnung\b|\bkonstituierende\b", re.I),
     "DE-NW-opening", CORE_OPENING),
    (re.compile(r"\bSitzungs(ende|schluss)\b|\bSchluss\s+der\s+Sitzung\b", re.I),
     "DE-NW-closing", CORE_CLOSING),
    (re.compile(r"\bAbstimmung\b", re.I),
     "DE-NW-voting", CORE_VOTING),
    (re.compile(r"\bHaushalts(?:gesetz|plan|begleitgesetz)\b|\bEinzelplan\b|"
                r"\bNachtragshaushalt\b", re.I),
     "DE-NW-budget", CORE_BUDGET),
]


def classify_de_nw(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify Landtag NRW agenda by the mediathek TOP (``e-top__title``) title."""
    return _match_title(title, _DE_NW_PATTERNS)


# ---------------------------------------------------------------------------
# DE-NI — Niedersächsischer Landtag (Plenar-TV API subject metadata)
# ---------------------------------------------------------------------------

# DE-NI subjects carry structured fields from the Plenar-TV REST API: a free-text
# ``title``, a ``subjectArt`` (Gesetzentwurf / Antrag / Große Anfrage / …) and a
# ``consultationType`` (Erste/Abschließende Beratung, …). We classify the combined
# string; first match wins. Procedural and budget markers come from the title;
# the ``subjectArt`` buckets the substantive items (bill / motion / interpellation
# / report / recommendation). Plain readings fall through to CORE_REGULAR.
_DE_NI_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bBefragung\s+der\s+Landesregierung\b", re.I),
     "DE-NI-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\b(Mündliche|Muendliche)\s+Anfragen\b|\bFragestunde\b", re.I),
     "DE-NI-question_time", CORE_QA),
    (re.compile(r"\b(Große|Grosse|Dringliche)\s+Anfrage\b", re.I),
     "DE-NI-interpellation", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bAktuelle\s+Stunde\b", re.I),
     "DE-NI-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-NI-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:der|des|und|zum|zur|von)\b", re.I),
     "DE-NI-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid|Verpflichtung)\b", re.I),
     "DE-NI-oath", CORE_OATH),
    (re.compile(r"\bGedenk(?:en|worte|stunde)\b|\bNachruf\b|\bW[üu]rdigung\b", re.I),
     "DE-NI-condolence", CORE_CONDOLENCE),
    (re.compile(r"\bMitteilungen\b|\bEr[öo]ffnung\b", re.I),
     "DE-NI-announcements", CORE_PROCEDURAL),
    (re.compile(r"\bHaushalts(?:gesetz|plan|begleitgesetz|beratungen)\b|\bEinzelplan\b|"
                r"\bNachtragshaushalt\b|\bHaushaltsgesetz\b", re.I),
     "DE-NI-budget", CORE_BUDGET),
    (re.compile(r"\bUnterrichtung\b", re.I),
     "DE-NI-unterrichtung", CORE_REPORT),
    (re.compile(r"\bBeschlussempfehlung\b", re.I),
     "DE-NI-beschlussempfehlung", CORE_RECOMMENDATION),
    (re.compile(r"\bGesetzentwurf\b", re.I),
     "DE-NI-gesetzentwurf", CORE_REGULAR),
    (re.compile(r"\bAntrag\b", re.I),
     "DE-NI-antrag", CORE_REGULAR),
]


def classify_de_ni(title: Optional[str], subject_art: Optional[str] = None,
                   consultation_type: Optional[str] = None) -> tuple[Optional[str], str]:
    """Classify a Niedersächsischer Landtag agenda item.

    Matches the combined ``title | subjectArt | consultationType`` string from
    the Plenar-TV API; first pattern wins. Returns ``(native_type, core_type)``.
    """
    haystack = " | ".join(x for x in (title, subject_art, consultation_type) if x)
    if not haystack:
        return None, CORE_REGULAR
    for pat, native, core in _DE_NI_PATTERNS:
        if pat.search(haystack):
            return native, core
    return None, CORE_REGULAR


# ---------------------------------------------------------------------------
# DE-SN — Sächsischer Landtag (mediathek per-speech theme / TOP text)
# ---------------------------------------------------------------------------

# The Saxony mediathek list item carries only a short per-speech ``thema`` text
# (e.g. "Vor Eintritt in die Tagesordnung", "Aktuelle Debatte: …", a bill title)
# plus a speech-time category ("Sonderredezeit", "Aktuelle Debatte", "Debatte",
# "Kurzintervention"). We classify the combined ``thema | speechType`` string;
# first match wins. Substantive readings/motions fall through to CORE_REGULAR.
_DE_SN_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bBefragung\s+der\s+Staatsregierung\b", re.I),
     "DE-SN-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bFragestunde\b|\bM[üu]ndliche\s+Anfragen\b", re.I),
     "DE-SN-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+(?:Debatte|Stunde)\b", re.I),
     "DE-SN-current_affairs", CORE_CURRENT_AFFAIRS),
    (re.compile(r"\bRegierungserkl[äa]rung\b", re.I),
     "DE-SN-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:der|des|und|zum|zur|von)\b", re.I),
     "DE-SN-election", CORE_ELECTION),
    (re.compile(r"\b(Vereidigung|Eidesleistung|Amtseid|Verpflichtung)\b", re.I),
     "DE-SN-oath", CORE_OATH),
    (re.compile(r"\bGedenk(?:en|worte|stunde)\b|\bNachruf\b|\bW[üu]rdigung\b", re.I),
     "DE-SN-condolence", CORE_CONDOLENCE),
    (re.compile(r"\b(Sitzungs)?Er[öo]ffnung\b|\bkonstituierende\b|"
                r"\bVor\s+Eintritt\s+in\s+die\s+Tagesordnung\b", re.I),
     "DE-SN-opening", CORE_OPENING),
    (re.compile(r"\bAbstimmung\b", re.I),
     "DE-SN-voting", CORE_VOTING),
    (re.compile(r"\bHaushalts(?:gesetz|plan|begleitgesetz)\b|\bEinzelplan\b|"
                r"\bNachtragshaushalt\b|\bDoppelhaushalt\b", re.I),
     "DE-SN-budget", CORE_BUDGET),
]


def classify_de_sn(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify a Sächsischer Landtag agenda item by its mediathek thema text."""
    return _match_title(title, _DE_SN_PATTERNS)


# ---------------------------------------------------------------------------
# SE — Riksdag kammaraktivitet
# ---------------------------------------------------------------------------

# Mapping documented at https://data.riksdagen.se. Add as new values appear
# in the source data — the default falls through to CORE_REGULAR.
_SE_KAMMARAKTIVITET_MAP: dict[str, str] = {
    "ärendedebatt": CORE_REGULAR,        # bill / item debate
    "frågestund": CORE_QA,               # Q&A
    "frågestund med statsminister": CORE_QA,
    "interpellationsdebatt": CORE_QA,    # interpellation Q&A
    "aktuell debatt": CORE_CURRENT_AFFAIRS,
    "regeringsförklaring": CORE_GOVERNMENT_DECLARATION,
    "votering": CORE_VOTING,
    "val": CORE_ELECTION,
}


def classify_se(kammaraktivitet: Optional[str]) -> tuple[Optional[str], str]:
    """Classify a Riksdag agenda by `kammaraktivitet`."""
    if not kammaraktivitet:
        return None, CORE_REGULAR
    key = kammaraktivitet.strip().lower()
    return kammaraktivitet, _SE_KAMMARAKTIVITET_MAP.get(key, CORE_REGULAR)


# ---------------------------------------------------------------------------
# ES — Congreso de los Diputados (OBJETOINICIATIVA + FASE)
# ---------------------------------------------------------------------------

# Ordered (regex, native, core). Matched case-insensitively against the
# combined "OBJETOINICIATIVA | FASE" string; first hit wins, so more specific
# patterns (pregunta, presupuesto) precede the broad bill/regular catch-alls.
_ES_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r'juramento o promesa', re.I), "ES-juramento", CORE_OATH),
    (re.compile(r'candidato a la presidencia del gobierno', re.I), "ES-investidura", CORE_ELECTION),
    (re.compile(r'reforma del reglamento', re.I), "ES-reglamento", CORE_RULES_OF_PROCEDURE),
    (re.compile(r'objetivos de estabilidad presupuestaria|techo de gasto|presupuestos generales del estado', re.I), "ES-presupuesto", CORE_BUDGET),
    (re.compile(r'declaraci[oó]n institucional', re.I), "ES-declaracion_institucional", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r'moci[oó]n consecuencia de interpelaci[oó]n', re.I), "ES-mocion", CORE_REGULAR),
    (re.compile(r'interpelaci[oó]n', re.I), "ES-interpelacion", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r'pregunta', re.I), "ES-pregunta", CORE_QA),
    (re.compile(r'solicitud de comparecencia|comparecencia', re.I), "ES-comparecencia", CORE_BRIEFING),
    (re.compile(r'proyecto de ley|proposici[oó]n de ley|proposici[oó]n de reforma|convalidaci[oó]n|real decreto-ley', re.I), "ES-ley", CORE_REGULAR),
]


def classify_es(objeto: Optional[str], fase: Optional[str] = None,
                tipo: Optional[str] = None) -> tuple[Optional[str], str]:
    """Classify a Congreso agenda from OBJETOINICIATIVA / FASE / TIPOINTERVENCION."""
    if tipo and tipo.strip().lower() == "votación":
        return "ES-votacion", CORE_VOTING
    haystack = f"{objeto or ''} | {fase or ''}"
    for pat, native, core in _ES_PATTERNS:
        if pat.search(haystack):
            return native, core
    # FASE fallback: a Q&A turn whose objeto didn't match above.
    if fase and "pregunta" in fase.strip().lower():
        return "ES-pregunta", CORE_QA
    return None, CORE_REGULAR


# ---------------------------------------------------------------------------
# EU — European Parliament (CRE rubric titles)
# ---------------------------------------------------------------------------

# CRE agenda headings appear in English on the *_EN.html doc, regardless of
# spoken-language preservation in speech bodies. Ordered (regex, native, core);
# first match wins. Matched against the agendaItem officialTitle.
_EU_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r'opening of the sitting', re.I), "EU-opening", CORE_OPENING),
    (re.compile(r'resumption of the sitting', re.I), "EU-resumption", CORE_OPENING),
    (re.compile(r'closure of the sitting', re.I), "EU-closing", CORE_CLOSING),
    (re.compile(r'one[- ]minute speeches', re.I), "EU-one_minute_speeches", CORE_REGULAR),
    (re.compile(r'question time', re.I), "EU-question_time", CORE_QA),
    (re.compile(r'(?:explanations of vote|explanation of vote)', re.I), "EU-explanations_of_vote", CORE_VOTING),
    (re.compile(r'\bvoting time\b|\bvote\b\s*$|\(vote\)', re.I), "EU-voting", CORE_VOTING),
    (re.compile(r'corrections to votes|voting intentions', re.I), "EU-vote_corrections", CORE_VOTING),
    (re.compile(r'formal sitting', re.I), "EU-formal_sitting", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r'address by', re.I), "EU-address", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r'statements? by (?:the|its) president', re.I), "EU-presidential_statement", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r'commission statement|council statement|statement (?:by|from) the (?:commission|council)', re.I),
     "EU-institutional_statement", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r'order of business', re.I), "EU-order_of_business", CORE_PROCEDURAL),
    (re.compile(r'composition of (?:parliament|committees)', re.I), "EU-composition", CORE_PROCEDURAL),
    (re.compile(r'verification of credentials', re.I), "EU-credentials", CORE_PROCEDURAL),
    (re.compile(r'membership of (?:committees|delegations)', re.I), "EU-membership", CORE_PROCEDURAL),
    (re.compile(r'agenda|amendment to the agenda', re.I), "EU-agenda", CORE_PROCEDURAL),
    (re.compile(r'transfer of appropriations|implementation of', re.I), "EU-procedural", CORE_PROCEDURAL),
    (re.compile(r'topical debate', re.I), "EU-topical_debate", CORE_CURRENT_AFFAIRS),
    (re.compile(r'debates? on cases of breaches of human rights', re.I), "EU-human_rights_debate", CORE_REGULAR),
    (re.compile(r'\(debate\)\s*$|\bdebate\b', re.I), "EU-debate", CORE_REGULAR),
    (re.compile(r'\(consultation\)\s*$', re.I), "EU-consultation", CORE_REGULAR),
    (re.compile(r'(?:annual|special|own[- ]initiative) report', re.I), "EU-report", CORE_REPORT),
    (re.compile(r'recommendation', re.I), "EU-recommendation", CORE_RECOMMENDATION),
    (re.compile(r'budget', re.I), "EU-budget", CORE_BUDGET),
    (re.compile(r'election of', re.I), "EU-election", CORE_ELECTION),
    (re.compile(r'oath|solemn declaration', re.I), "EU-oath", CORE_OATH),
    (re.compile(r'condolences?|in memoriam', re.I), "EU-condolence", CORE_CONDOLENCE),
    (re.compile(r'rules of procedure', re.I), "EU-rules_of_procedure", CORE_RULES_OF_PROCEDURE),
    (re.compile(r'negotiations ahead|first reading|second reading|third reading', re.I),
     "EU-legislative_procedure", CORE_REGULAR),
    (re.compile(r'announcement', re.I), "EU-announcement", CORE_PROCEDURAL),
]


def classify_eu_native(official_title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify an EU CRE agenda item by its English official title.

    Returns ``(native_type, core_type)`` where ``native_type`` is an ``EU-*``
    token and ``core_type`` is one of the ``CORE_*`` constants. Falls back to
    ``(None, CORE_REGULAR)`` when no pattern matches — CRE titles are
    free-form so the fall-through rate is non-trivial.
    """
    return _match_title(official_title, _EU_PATTERNS)


# ---------------------------------------------------------------------------
# NO — Stortinget (free-text saktittel)
# ---------------------------------------------------------------------------

# Stortinget does not publish a structured agenda-type field; the
# ``Saktittel`` element carries free Norwegian Bokmål prose. Patterns are
# matched case-insensitively; first match wins.
_NO_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r'\btrontaledebatt(?:en)?\b', re.I), "NO-trontaledebatt", CORE_REGULAR),
    (re.compile(r'\bspørretime\b|\bspørretimespørsmål\b', re.I), "NO-sporretime", CORE_QA),
    (re.compile(r'\bmuntlig spørretime\b', re.I), "NO-muntlig_sporretime", CORE_QA),
    (re.compile(r'\binterpellasjon\b', re.I), "NO-interpellasjon", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r'\bstatsbudsjett(?:et)?\b|\bnasjonalbudsjett(?:et)?\b', re.I), "NO-budsjett", CORE_BUDGET),
    (re.compile(r'\bvotering\b', re.I), "NO-votering", CORE_VOTING),
    (re.compile(r'\bvalg\b', re.I), "NO-valg", CORE_ELECTION),
    (re.compile(r'\bredegjørelse\b', re.I), "NO-redegjorelse", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r'\breferat\b', re.I), "NO-referat", CORE_PROCEDURAL),
    (re.compile(r'\brepresentantforslag\b', re.I), "NO-representantforslag", CORE_REGULAR),
    (re.compile(r'\b(?:innstilling|lovforslag)\b', re.I), "NO-lovforslag", CORE_REGULAR),
    (re.compile(r'\bdagsorden\b', re.I), "NO-dagsorden", CORE_PROCEDURAL),
]


def classify_no(saktittel: Optional[str]) -> tuple[Optional[str], str]:
    """Classify a Storting agenda by free-text ``Saktittel``."""
    return _match_title(saktittel, _NO_PATTERNS)


# ---------------------------------------------------------------------------
# AT — Nationalrat (Mediathek ``debatte.content`` titles)
# ---------------------------------------------------------------------------

# Matched case-insensitively against the raw Mediathek agenda title (e.g.
# "TOP 7 Nächtliche Dauerbeleuchtung von Windrädern", "Abstimmung über die
# Tagesordnungspunkte 1 bis 5", "Dringliche Anfrage an den Innenminister").
# First match wins. The bulk are "TOP N …" substantive debate items that fall
# through to AT-top / CORE_REGULAR.
#
# Tuning note: patterns must NOT misfire on substantive "TOP N …" titles that
# merely mention a keyword. Confirmed collisions on the live corpus drove the
# narrow forms here — bare "Budget" appears in debate titles ("… im Budget
# 2021-2024") so we key on "Budgetrede"/"Bundesfinanzgesetz"; bare "Gedenken"
# appears substantively ("Gedenken an den Völkermord") so condolence keys on
# "Gedenkminute"/"Trauerkundgebung"/"Schweigeminute"; "Wahl" is gated to
# "Wahl eines/einer/des/der …"; and the government-declaration "Erklärung der/
# des …" excludes "… des Präsidenten" (a chair statement, not a declaration).
_AT_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bAbstimmung", re.I),  # also "Abstimmungsvorgang"
     "AT-voting", CORE_VOTING),
    (re.compile(r"\bFragestunde\b", re.I),
     "AT-question_time", CORE_QA),
    (re.compile(r"\bAktuelle\s+(?:Europa)?stunde\b", re.I),
     "AT-current_affairs", CORE_CURRENT_AFFAIRS),
    # Urgent interpellation to a minister/the government.
    (re.compile(r"\bDringliche?\s+Anfrage\b|\bDringl\.?\s+Anfrage\b", re.I),
     "AT-urgent_question", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r"\bDringlicher\s+Antrag\b", re.I),
     "AT-urgent_motion", CORE_REGULAR),
    (re.compile(r"\bAngelobung\b", re.I),
     "AT-oath", CORE_OATH),
    (re.compile(r"\b(?:Trauerkundgebung|Gedenkminute|Gedenkstunde|Schweigeminute)\b", re.I),
     "AT-condolence", CORE_CONDOLENCE),
    (re.compile(r"\bSitzungsunterbrechung\b", re.I),
     "AT-session_break", CORE_PROCEDURAL),
    (re.compile(r"\bPräsidium\b", re.I),
     "AT-presidency", CORE_PROCEDURAL),
    # Prefix match (no trailing \b) so plurals are caught: "Wortmeldungen",
    # "Einwendungen". Anchored tail (^…) covers the rare procedural stubs that
    # would otherwise collide inside a TOP subject (e.g. "ÖH-Vertretungen").
    (re.compile(r"\b(?:Ordnungsruf|Mandatsverzicht|Einwendung|Wortmeldung)", re.I),
     "AT-procedural", CORE_PROCEDURAL),
    (re.compile(r"^(?:Verlesung|Verlangen|Zuweisung|Vertretung|Anträge\s+gemäß)", re.I),
     "AT-procedural", CORE_PROCEDURAL),
    (re.compile(r"\bEinberufung\b|\bBeschluss\s+auf\s+Beendigung\b", re.I),
     "AT-session_convocation", CORE_PROCEDURAL),
    (re.compile(r"\bSchlussansprache\b", re.I),
     "AT-closing_address", CORE_CLOSING),
    # Chair statement/address (kept distinct from a government declaration).
    (re.compile(r"\b(?:Ansprache|Erklärung)\s+(?:des|der)\s+Präsident", re.I),
     "AT-presidential_statement", CORE_OTHER),
    (re.compile(r"\bBudgetrede\b|\bBundesfinanz(?:rahmen)?gesetz\b", re.I),
     "AT-budget", CORE_BUDGET),
    (re.compile(r"\bErklärung(?:en)?\s+(?:der|des)\s+(?!Präsident)", re.I),
     "AT-government_declaration", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"\bWahl\s+(?:eines|einer|des|der)\b", re.I),
     "AT-election", CORE_ELECTION),
    (re.compile(r"\bKurze\s+Debatte\b|\bKurzdebatte\b", re.I),
     "AT-short_debate", CORE_REGULAR),
    (re.compile(r"^TOP\s+\d", re.I),
     "AT-top", CORE_REGULAR),
]


def classify_at(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify a Nationalrat agenda by its Mediathek ``debatte.content`` title."""
    return _match_title(title, _AT_PATTERNS)


# ---------------------------------------------------------------------------
# FI — Eduskunta (PTK agenda-item titles / käsittely vocabulary)
# ---------------------------------------------------------------------------

# Finnish plenary agenda items (Asiakohta) carry a free-text Finnish title plus
# a "käsittely" (reading) stage. Patterns are matched case-insensitively against
# the agenda title; first match wins. Substantive items (Hallituksen esitys,
# lähetekeskustelu, käsittely) fall through to CORE_REGULAR.
_FI_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r'\bnimenhuuto\b', re.I), "FI-nimenhuuto", CORE_PROCEDURAL),
    (re.compile(r'istunnon\s+avaus|\bavataan\s+istunto', re.I), "FI-avaus", CORE_OPENING),
    (re.compile(r'\bsuullinen\s+kyselytunti\b', re.I), "FI-suullinen_kyselytunti", CORE_QA),
    (re.compile(r'\bkyselytunti\b|\bkirjalli(nen|set)\s+kysymy', re.I), "FI-kyselytunti", CORE_QA),
    (re.compile(r'\bvälikysymys\b', re.I), "FI-valikysymys", CORE_GOVERNMENT_QUESTIONING),
    (re.compile(r'\bajankohtaiskeskustelu\b', re.I), "FI-ajankohtaiskeskustelu", CORE_CURRENT_AFFAIRS),
    (re.compile(r'pääministerin\s+ilmoitus|valtioneuvoston\s+(tiedonanto|ilmoitus|selonteko)', re.I),
     "FI-paaministerin_ilmoitus", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r'talousarvio|lisätalousarvio|valtion\s+talousarvio', re.I), "FI-talousarvio", CORE_BUDGET),
    (re.compile(r'\bvaali\b|\bvaalit\b|valitaan|toimitetaan\s+.*vaali', re.I), "FI-vaali", CORE_ELECTION),
    (re.compile(r'\bäänesty', re.I), "FI-aanestys", CORE_VOTING),
    (re.compile(r'juhlallinen\s+vakuutus|\bvakuutus\b|\bvala\b', re.I), "FI-vakuutus", CORE_OATH),
    (re.compile(r'suruvalittelu|vaiti.{0,3}olo|muistosanat', re.I), "FI-suruvalittelu", CORE_CONDOLENCE),
    (re.compile(r'työjärjestys|menettelytapa', re.I), "FI-tyojarjestys", CORE_RULES_OF_PROCEDURE),
    (re.compile(r'istunnon\s+päättä|\bpäätetään\s+istunto', re.I), "FI-paattaminen", CORE_CLOSING),
    (re.compile(r'\bselonteko\b|\bkertomus\b', re.I), "FI-kertomus", CORE_REPORT),
    (re.compile(r'lähetekeskustelu', re.I), "FI-lahetekeskustelu", CORE_REGULAR),
    (re.compile(r'(ensimmäinen|toinen|ainoa)\s+käsittely|\bkäsittely\b', re.I), "FI-kasittely", CORE_REGULAR),
    (re.compile(r'hallituksen\s+esitys|lakialoite|toimenpidealoite', re.I), "FI-esitys", CORE_REGULAR),
]


def classify_fi(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify an Eduskunta agenda item by its Finnish title text."""
    return _match_title(title, _FI_PATTERNS)


# ---------------------------------------------------------------------------
# FR — Assemblée nationale (Syceron point titles / code_grammaire)
# ---------------------------------------------------------------------------

# French plenary agenda items come from the compte-rendu ``<point>`` titles
# (free French prose). Patterns are matched case-insensitively against the
# agenda title; first match wins. Substantive bills ("projet de loi",
# "discussion des articles") fall through to CORE_REGULAR.
_FR_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"questions?\s+au\s+gouvernement", re.I), "FR-questions_au_gouvernement", CORE_QA),
    (re.compile(r"questions?\s+orales", re.I), "FR-questions_orales", CORE_QA),
    (re.compile(r"d[ée]claration\s+(?:du\s+gouvernement|de\s+politique\s+g[ée]n[ée]rale)", re.I),
     "FR-declaration_gouvernement", CORE_GOVERNMENT_DECLARATION),
    (re.compile(r"motion\s+de\s+censure", re.I), "FR-motion_de_censure", CORE_REGULAR),
    (re.compile(r"explications?\s+de\s+vote|scrutin\s+public|mise\s+aux\s+voix", re.I),
     "FR-scrutin", CORE_VOTING),
    (re.compile(r"\b[ée]lection\s+(?:du|de\s+la|des)\b", re.I), "FR-election", CORE_ELECTION),
    (re.compile(r"[ée]loge\s+fun[èe]bre|hommage", re.I), "FR-eloge_funebre", CORE_CONDOLENCE),
    (re.compile(r"ouverture\s+de\s+la\s+(?:session|s[ée]ance)", re.I), "FR-ouverture", CORE_OPENING),
    (re.compile(r"cl[ôo]ture\s+de\s+la\s+session", re.I), "FR-cloture", CORE_CLOSING),
    (re.compile(r"rappels?\s+au\s+r[èe]glement|modification\s+de\s+l['’]ordre\s+du\s+jour",
                re.I), "FR-rappel_au_reglement", CORE_RULES_OF_PROCEDURE),
    (re.compile(r"projet\s+de\s+loi\s+de\s+finances|loi\s+de\s+finances|budget", re.I),
     "FR-budget", CORE_BUDGET),
    (re.compile(r"ordre\s+du\s+jour", re.I), "FR-ordre_du_jour", CORE_PROCEDURAL),
    (re.compile(r"suspension\s+et\s+reprise\s+de\s+la\s+s[ée]ance|suspension\s+de\s+la\s+s[ée]ance",
                re.I), "FR-suspension", CORE_PROCEDURAL),
    (re.compile(r"discussion\s+g[ée]n[ée]rale|discussion\s+des\s+articles|"
                r"projet\s+de\s+loi|proposition\s+de\s+(?:loi|r[ée]solution)", re.I),
     "FR-discussion", CORE_REGULAR),
]


def classify_fr(title: Optional[str]) -> tuple[Optional[str], str]:
    """Classify an Assemblée nationale agenda item by its French ``<point>`` title."""
    return _match_title(title, _FR_PATTERNS)


# ---------------------------------------------------------------------------
# PT — Assembleia da República (av.parlamento.pt interventionType vocabulary)
# ---------------------------------------------------------------------------

# Unlike the others, the PT classification key is the per-speech
# ``interventionType`` from the av.parlamento.pt JSON — a controlled vocabulary
# (Abertura da sessão, Votações, Intervenção, Pedido de esclarecimento, …), not a
# free-text agenda title. Matched case/accent-insensitively; first match wins;
# a plain "Intervenção" (substantive speech) falls through to CORE_REGULAR.
_PT_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"abertura", re.I), "PT-abertura", CORE_OPENING),
    (re.compile(r"encerramento", re.I), "PT-encerramento", CORE_CLOSING),
    (re.compile(r"vota", re.I), "PT-votacoes", CORE_VOTING),
    (re.compile(r"leitura", re.I), "PT-leitura", CORE_PROCEDURAL),
    (re.compile(r"interpela", re.I), "PT-interpelacao_a_mesa", CORE_PROCEDURAL),
    (re.compile(r"protesto", re.I), "PT-protesto", CORE_PROCEDURAL),
    (re.compile(r"ponto\s+de\s+ordem", re.I), "PT-ponto_de_ordem", CORE_PROCEDURAL),
    (re.compile(r"defesa\s+da?\s+honra|defesa\s+da\s+consist", re.I),
     "PT-defesa_da_honra", CORE_PROCEDURAL),
    (re.compile(r"pedido\s+de\s+esclarecimento|esclarecimento|resposta", re.I),
     "PT-pedido_de_esclarecimento", CORE_QA),
    (re.compile(r"declara", re.I), "PT-declaracao_politica", CORE_REGULAR),
    (re.compile(r"interven", re.I), "PT-intervencao", CORE_REGULAR),
]


def classify_pt(intervention_type: Optional[str]) -> tuple[Optional[str], str]:
    """Classify a PT speech by its av.parlamento.pt ``interventionType``."""
    return _match_title(intervention_type, _PT_PATTERNS)


# ---------------------------------------------------------------------------
# Convenience: write classification onto an agendaItem dict
# ---------------------------------------------------------------------------

def annotate_agenda_item(agenda_item: dict, native_type: Optional[str],
                         core_type: str) -> None:
    """Set ``type`` and ``nativeType`` on an agendaItem dict in place.

    Existing values are preserved if non-empty (so a parliament-specific
    parser that already wrote a value wins over a generic re-classification).
    """
    if "type" not in agenda_item or not agenda_item.get("type"):
        agenda_item["type"] = core_type
    if native_type and not agenda_item.get("nativeType"):
        agenda_item["nativeType"] = native_type
