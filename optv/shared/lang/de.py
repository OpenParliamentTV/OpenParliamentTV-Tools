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
    # "<Parliament> – N. Wahlperiode – M. Sitzung – <date>" (BW, SN, …). The
    # "N. Wahlperiode – M. Sitzung" run is header-specific; real speech that
    # merely names the parliament ("…des 17. Landtags von Baden-Württemberg")
    # lacks it, so this does not eat substantive text.
    if re.search(r"\d+\.\s*Wahlperiode\s*[–-]\s*\d+\.\s*Sitzung\b", t):
        return True
    # NW footer "Landtag <date> Nordrhein-Westfalen <page> Plenarprotokoll N/M".
    if re.search(r"Nordrhein-Westfalen\s+\d+\s+Plenarprotokoll\s+\d+/\d+", t):
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


# --- PDF→TEI German sentence/text heuristics --------------------------------
# Consumed by the parliament-agnostic core in optv/shared/pdf2tei/tei2json.py:
# sentence segmentation, non-speech (vote-list/table) detection, and cross-block
# de-hyphenation. The mechanics are generic; the German knowledge lives here.

ORDINAL_NOUNS = ("Wahlperiode", "Sitzung", "Lesung", "Beratung", "Wahlgang",
                 "Legislaturperiode", "Tagesordnungspunkt", "Absatz", "Artikel",
                 "Nummer", "Halbsatz", "Spiegelstrich")
# Abbreviations that take a trailing period and are never a sentence end (some
# multi-part: "z. B.", "d. h.").
SENTENCE_ABBREVIATIONS = (
    "Abs", "Art", "Bd", "bzw", "ca", "d.h", "Dr", "ebd", "etc", "evtl", "ff",
    "ggf", "Hrsg", "i.d.R", "inkl", "Mio", "Mrd", "Nr", "Nrn", "Pos", "Prof",
    "Rn", "sog", "Tz", "u.a", "u.Ä", "usw", "v.a", "vgl", "z.B", "z.T", "Ziff")

_SENT_PH = "\x00"   # placeholder for a protected (non-boundary) period
_SENT_PROTECT_RES = (
    [re.compile(r"\b" + r"\.\s*".join(re.escape(p) for p in ab.split(".")) + r"\.")
     for ab in SENTENCE_ABBREVIATIONS]
    + [re.compile(r"\b[A-ZÄÖÜ]\.(?=\s+[A-ZÄÖÜ])"),   # single-letter initial
       re.compile(rf"\b\d+\.(?=\s+(?:{'|'.join(MONTHS)}|{'|'.join(ORDINAL_NOUNS)})\b)")])
_SENT_SPLIT_RE = re.compile(r"(?<=[.!?…])\s+(?=[A-ZÄÖÜ„\"»])")


def regex_sentencize(text: str) -> list[str]:
    """Deterministic German sentence split (no model): break after .!?… + an
    uppercase start, but protect abbreviations, single-letter initials, and
    ordinal/date periods (``11. August``, ``17. Wahlperiode``) first."""
    text = (text or "").strip()
    if not text:
        return []
    for rx in _SENT_PROTECT_RES:
        text = rx.sub(lambda m: m.group(0).replace(".", _SENT_PH), text)
    return [s.replace(_SENT_PH, ".").strip()
            for s in _SENT_SPLIT_RE.split(text) if s.strip()]


_nlp = None


def spacy_sentencize(text: str) -> list[str]:
    """German sentence split via spaCy's deterministic rule-based ``sentencizer``
    — the same component the DE Bundestag parser uses, so the whole DE tier is
    consistent. The German tokenizer protects abbreviations/ordinals and it
    catches ?/!/interjection boundaries the bare regex misses. Loaded once per
    process; deterministic (no model weights — pin the spaCy version)."""
    text = (text or "").strip()
    if not text:
        return []
    global _nlp
    if _nlp is None:
        from spacy.lang.de import German
        _nlp = German()
        _nlp.add_pipe("sentencizer")
    sents = [str(s).strip() for s in _nlp(text).sents if str(s).strip()]
    # Length-gated secondary split of the over-long sentences the rule-based
    # sentencizer leaves whole (German ;/–/: clause chains, enumerations).
    return split_long_sentences(sents)


# Comma-introduced German clause starters: subordinating conjunctions, the
# conditional apodosis "dann", and unambiguous relative pronouns. Used by the
# length-gated splitter to break over-long subordinate-clause / conditional
# chains ("…, wenn …, wenn …, dann …" / "…, dass …, dass …") at real clause
# boundaries instead of arbitrary commas. Bare articles der/die/das are
# *excluded* — too ambiguous with enumeration/apposition commas — keeping only
# the longer relative forms (deren/dessen/welche…).
_CLAUSE_STARTERS = (
    "dass", "weil", "wenn", "ob", "obwohl", "obgleich", "während", "sodass",
    "sobald", "solange", "indem", "sofern", "insofern", "falls", "damit",
    "nachdem", "bevor", "seitdem", "seit", "soweit", "zumal", "wohingegen",
    "wobei", "worunter", "woraufhin", "sondern", "dann",
    "deren", "dessen", "welche", "welcher", "welches", "welchem", "welchen",
)
_CLAUSE_RE = re.compile(
    r",\s+(?=(?:" + "|".join(_CLAUSE_STARTERS) + r")\b)")


def split_long_sentence(text: str, **kwargs) -> list[str]:
    """German length-gated secondary split: :func:`optv.shared.sentence_split.
    split_long_sentence` with the German clause-boundary regex wired in. Used by
    every German sentencizer call site (proceedings2json, parlamint2json, and
    the AT / state-parliament path via spacy_sentencize) so they segment alike."""
    from optv.shared.sentence_split import split_long_sentence as _split
    kwargs.setdefault("clause_re", _CLAUSE_RE)
    return _split(text, **kwargs)


def split_long_sentences(texts, **kwargs) -> list[str]:
    """Flatten :func:`split_long_sentence` over an iterable of sentence texts."""
    out: list[str] = []
    for t in texts:
        out.extend(split_long_sentence(t, **kwargs))
    return out


# Non-speech blocks extracted as flowing text (roll-call vote lists, voter name
# lists, appendix tables). They have no real sentences, are not spoken in the
# chair's clip, and would corrupt the per-clip aeneas alignment — so we drop them.
_FACTION_ALT = "|".join(sorted(
    {re.escape(f) for f in FACTIONS}
    | {"BSW", "BÜNDNISGRÜNE", "FREIE WÄHLER", "fraktionslos"}, key=len, reverse=True))
_VOTE_APPENDIX_RE = re.compile(
    r"Anlage\s+(\d|zum Protokoll)|Umbesetzungen in (den|verschiedenen) Aussch|"
    r"\bMit\s+(Ja|Nein)\b[^.]{0,40}\bgestimmt\b|\bhaben\s+gestimmt\b\s*:|"
    r"namentliche[rn]?\s+Abstimmung|\b(Ja|Nein|Enthaltungs|Gesamt)stimmen\s*:?\s*\d",
    re.IGNORECASE)
_ROLLCALL_RE = re.compile(r"\([A-ZÄÖÜ][A-Za-zÄÖÜäöüß]*\)\s*x\b")   # "(CDU) x  Name"
_FACTION_LIST_RE = re.compile(rf"^({_FACTION_ALT})\s*:")            # "CDU: Name, …"
_NAME_TOKEN_RE = re.compile(r"\b[A-ZÄÖÜ][a-zäöüß]+\b")
_SENT_BOUNDARY_RE = re.compile(r"[.!?]\s+[A-ZÄÖÜ]")


def is_non_speech(text: str) -> bool:
    """True for roll-call vote lists / voter lists / appendix tables. Conservative
    — long blocks only, clear markers or a dense name list with no real sentence
    boundary (abbreviation periods like ``Dr.`` are not boundaries)."""
    t = (text or "").strip()
    if len(t) < 200:                       # never drop normal-length speech
        return False
    if _VOTE_APPENDIX_RE.search(t) or len(_ROLLCALL_RE.findall(t)) >= 5:
        return True
    if _FACTION_LIST_RE.match(t) and t.count(",") > 12:
        return True
    if (not _SENT_BOUNDARY_RE.search(t)
            and len(_NAME_TOKEN_RE.findall(t)) > 20 and t.count(",") > 10):
        return True
    return False


# Conjunctions that follow an elided compound ("Ein- und Ausgänge"): a hyphen
# before one is intentional, not a line-wrap, so keep it.
ELISION_CONJ = frozenset((
    "und", "oder", "bzw", "beziehungsweise", "sowie", "wie", "als", "aber",
    "noch", "samt", "nebst", "bis", "gegen", "respektive", "resp"))
_TRAILING_HYPHEN_RE = re.compile(r"[a-zäöüß]-$")


def join_segments(parts) -> str:
    """Join block/segment texts with a space, but reconnect a word split across a
    block boundary (column/page break): a part ending ``<lowercase>-`` then a next
    part starting lowercase is a wrap (within-block wraps are already handled by
    ``dehyphenate``). Elided compounds (next token a conjunction) are kept."""
    out = ""
    for p in parts:
        p = (p or "").strip()
        if not p:
            continue
        if not out:
            out = p
            continue
        nxt = p.split(" ", 1)[0]
        if (_TRAILING_HYPHEN_RE.search(out) and nxt[:1].islower()
                and nxt.rstrip(",.;:!?").lower() not in ELISION_CONJ):
            out = out[:-1] + p
        else:
            out = out + " " + p
    return out
