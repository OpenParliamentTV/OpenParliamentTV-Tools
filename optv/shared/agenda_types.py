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
    if not title:
        return None, CORE_REGULAR
    for pat, native, core in _DE_NATIVE_PATTERNS:
        if pat.search(title):
            return native, core
    return None, CORE_REGULAR


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
    if not title:
        return None, CORE_REGULAR
    for pat, native, core in _DE_RP_PATTERNS:
        if pat.search(title):
            return native, core
    return None, CORE_REGULAR


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
