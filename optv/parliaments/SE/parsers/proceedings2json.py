#! /usr/bin/env python3
"""
Parse a Riksdag session bundle into Stage-2-shaped proceedings JSON.

Input: ``original/proceedings/{session}-anforanden.json``
   the bundle produced by the scraper:
   ``{"protokoll": {...dokument fields...}, "anforanden": [<full anforande>, ...]}``

Output: ``original/proceedings/{session}-proceedings.json``
   ``{"meta": {...}, "data": [<speech>, ...]}``
   Per-speech entries follow the OPTV Stage 2 shape but **omit `media` and
   `dateStart`** — those are filled in by the merger from per-debate
   metadata. Other Stage-2 required fields (parliament, electoralPeriod,
   session, agendaItem, people, textContents) are present.

Sentence segmentation uses spaCy's ``sv_core_news_md`` model (declared in
``optv/parliaments/SE/manifest.yaml``). HTML in ``anforandetext`` is
stripped to plain text per ``<p>`` paragraph before sentencizing.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

# Allow ./proceedings2json.py and python -m optv.parliaments.SE.parsers.proceedings2json.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.SE.parsers"

import json
import lxml.html

from optv.parliaments.SE.common import Config
from optv.shared.agenda_types import classify_se
from optv.parliaments import get_rights as _get_rights
from optv.parliaments import get_language as _get_language

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

LANGUAGE_CODE = _get_language("SE")
PARLIAMENT_CODE = "SE"
SPEECH_CREATOR = _get_rights("SE", stream="proceedings")["creator"]
SPEECH_LICENSE = _get_rights("SE", stream="proceedings")["license"]

# Strip a trailing party tag like " (SD)" or " (KD)" from `talare` to recover
# the speaker's name. Riksdag puts the party in parentheses at the end.
_PARTY_SUFFIX_RE = re.compile(r"\s*\(([^)]+)\)\s*$")

# Recognise role prefixes in `talare`. Mapping → OPTV speakerContext enum
# (see optv/shared/schema/stage2-full.schema.json#/definitions/speakerContext).
# Riksdag uses "Talmannen" (the speaker), "Förste/Andre/Tredje vice talmannen"
# (deputy speakers). The actual person's name follows the role.
_ROLE_PREFIXES: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"^Talmannen\b\s*", re.IGNORECASE), "talman", "president"),
    (re.compile(r"^Förste vice talmannen\b\s*", re.IGNORECASE), "förste vice talman", "vice-president"),
    (re.compile(r"^Andre vice talmannen\b\s*", re.IGNORECASE), "andre vice talman", "vice-president"),
    (re.compile(r"^Tredje vice talmannen\b\s*", re.IGNORECASE), "tredje vice talman", "vice-president"),
]

# Recognise ministerial-role prefixes (e.g. "Utrikesministern Maria Malmer
# Stenergard", "EU-minister Jessica Rosencrantz", "Energi- och näringsministern
# Ebba Busch", "Bistånds- och utrikeshandelsministern Benjamin Dousa"). The
# prefix is one or more compound words whose final word ends in "minister" or
# "ministern" (definite article), with words optionally joined by hyphens,
# commas, and the conjunction "och". The personal name follows. The role
# string is kept in `role` (for downstream display); the OPTV speakerContext
# enum has no "government" value, so context stays "main-speaker" — these
# *are* the substantive speaker for their turn.
_MINISTER_PREFIX_RE = re.compile(
    r"^(?P<role>[A-ZÅÄÖÉ][^\s]*ministern?"     # first compound ending in minister(n) — handles "Utrikesministern", "EU-minister"
    r"|[A-ZÅÄÖÉ][^\s]*(?:[\-,]?\s+(?:och\s+)?[a-zåäöéÅÄÖÉA-Z][^\s]*)*?ministern?)"  # OR multi-word title ending in minister(n)
    r"\s+(?P<name>[A-ZÅÄÖÉ][^\s]+(?:\s+[A-ZÅÄÖÉ][^\s]+)+)\s*$",  # ≥ 2 personal name tokens (allows ALL-CAPS)
)


def _strip_minister_prefix(s: str) -> tuple[str, str | None]:
    """Try to split ``s`` into (name, role) where role ends in "minister(n)".

    Returns ``(s, None)`` unchanged if no match.
    """
    m = _MINISTER_PREFIX_RE.match(s)
    if not m:
        return s, None
    return m.group("name").strip(), m.group("role").strip()


def parse_talare(talare: str) -> tuple[str, str | None, str]:
    """Split ``talare`` (e.g. ``"Dennis Dioukarev (SD)"`` or
    ``"Talmannen Andreas Norlén"``) into ``(label, role, context)``.

    - ``label`` is the cleaned person name with any trailing party tag and
      leading role prefix removed. When ``talare`` is *just* the role
      (e.g. ``"TREDJE VICE TALMANNEN"`` — Riksdag returns this for
      procedural interventions where the speaker is identified only by
      office), ``label`` falls back to the role name itself so the schema
      ``minLength: 1`` constraint is satisfied.
    - ``role`` is the Swedish role label (``"talman"`` etc.) or ``None`` for
      regular members.
    - ``context`` is the OPTV speakerContext enum value.
    """
    s = (talare or "").strip()
    # Strip trailing "(PARTY)".
    s = _PARTY_SUFFIX_RE.sub("", s).strip()
    # Detect talman role prefix.
    for pat, role_label, ctx in _ROLE_PREFIXES:
        m = pat.match(s)
        if m:
            name = s[m.end():].strip()
            if not name:
                # Anonymous procedural intervention — keep the role as label.
                # Title-case "TREDJE VICE TALMANNEN" → "Tredje vice talmannen".
                name = s.capitalize() if s.isupper() else s
            return name, role_label, ctx
    # Detect ministerial role prefix. Keeps `context` at "main-speaker" (the
    # OPTV enum has no "government" value), but moves the role into `role`
    # so NEL can match the personal name against Wikidata's canonical labels.
    name, role = _strip_minister_prefix(s)
    if role:
        return name, role, "main-speaker"
    return s, None, "main-speaker"


def html_to_paragraphs(anforandetext: str) -> list[str]:
    """Extract plain-text paragraphs from Riksdag's HTML ``anforandetext``.

    The text is wrapped in ``<p>...</p>`` blocks. Returns one string per
    paragraph with whitespace collapsed; empty paragraphs are dropped.
    """
    if not anforandetext:
        return []
    # Wrap in a single root element so lxml can parse fragments uniformly.
    root = lxml.html.fragment_fromstring(f"<div>{anforandetext}</div>")
    paragraphs: list[str] = []
    # If the input already has <p> children we want each one; otherwise the
    # whole thing is one paragraph.
    p_elements = root.findall(".//p")
    if p_elements:
        for p in p_elements:
            text = re.sub(r"\s+", " ", p.text_content()).strip()
            if text:
                paragraphs.append(text)
    else:
        text = re.sub(r"\s+", " ", root.text_content()).strip()
        if text:
            paragraphs.append(text)
    return paragraphs


def split_sentences(nlp, text: str) -> list[str]:
    """Sentence-split ``text`` with spaCy. Empty / whitespace input → []."""
    text = text.strip()
    if not text:
        return []
    doc = nlp(text)
    return [s.text.strip() for s in doc.sents if s.text.strip()]


def build_textbody(nlp, paragraphs: list[str], speaker_label: str, role: str | None) -> list[dict]:
    """Build the ``textBody`` array from speech paragraphs.

    Currently emits a single ``type: "speech"`` block whose ``sentences`` is
    the concatenation of all paragraph sentences. Riksdag does not embed
    interjections in the same way the Bundestag does (which is why DE's
    parser splits comments out into separate ``type: "comment"`` blocks);
    for SE everything inside ``anforandetext`` is the speaker.
    """
    sentences: list[dict] = []
    for para in paragraphs:
        for sent in split_sentences(nlp, para):
            sentences.append({"text": sent})
    return [{
        "type": "speech",
        "speaker": speaker_label,
        "speakerstatus": role,
        "sentences": sentences,
    }]


def parse_protokoll_date(prot: dict) -> datetime.date:
    """``protokoll.datum`` is e.g. ``"2026-03-17 00:00:00"``."""
    return datetime.datetime.strptime(prot["datum"], "%Y-%m-%d %H:%M:%S").date()


def speech_record(anf: dict, prot: dict, speech_index: int, nlp) -> dict:
    """Convert one anforande payload into a Stage-2-shaped speech dict."""
    rm = prot["rm"]                                    # "2025/26"
    period_number = int(rm.split("/")[0])
    session_number = int(prot["nummer"])               # 91
    protokoll_id = prot["dok_id"]                      # "HD0991"

    talare = anf.get("talare") or ""
    label, role, context = parse_talare(talare)
    parti = (anf.get("parti") or "").strip()

    avsnitt = (anf.get("avsnittsrubrik") or "").strip()
    underrubrik = (anf.get("underrubrik") or "").strip()
    fetch_id = anf.get("_fetch_id") or f"{protokoll_id}-{anf.get('anforande_nummer')}"

    paragraphs = html_to_paragraphs(anf.get("anforandetext") or "")
    text_body = build_textbody(nlp, paragraphs, label, role)

    person: dict[str, Any] = {
        "type": "memberOfParliament",
        "label": label,
        "context": context,
    }
    if role:
        person["role"] = role
    # Skip the faction *only* when speaking as talman/vice-talman: Riksdag
    # returns the role string in `parti` too (e.g. parti="TREDJE VICE
    # TALMANNEN"), which isn't a real party, and chairs are non-partisan.
    # Ministers (context == "main-speaker") ARE partisan and `parti` carries
    # their actual party code (M, KD, ...).
    is_chair_role = context in ("president", "vice-president", "interim-president")
    if parti and not is_chair_role:
        # Schema requires faction to be an object ({label, wid?, wtype?});
        # NEL fills in wid/wtype later.
        person["faction"] = {"label": parti}
    intressent = anf.get("intressent_id") or ""
    if intressent:
        person["originPersonID"] = intressent

    kammaraktivitet = anf.get("kammaraktivitet") or ""
    native_type, core_type = classify_se(kammaraktivitet)
    agenda_item: dict[str, Any] = {
        # Riksdag does not publish a separate "official title" for the
        # agenda item the way the Bundestag does (Tagesordnungspunkt N).
        # ``avsnittsrubrik`` is what appears as the section heading; we
        # use it for both fields. ``underrubrik`` carries any sub-title.
        "officialTitle": avsnitt,
        "title": (f"{avsnitt} – {underrubrik}".strip(" –") if underrubrik else avsnitt),
        "type": core_type,
    }
    if native_type:
        agenda_item["nativeType"] = native_type

    return {
        "parliament": PARLIAMENT_CODE,
        "electoralPeriod": {"number": period_number},
        "session": {"number": session_number},
        "agendaItem": agenda_item,
        "speechIndex": speech_index,
        "originID": fetch_id,
        "isReply": (anf.get("replik") == "J"),
        "people": [person],
        "textContents": [{
            "type": "proceedings",
            "language": LANGUAGE_CODE,
            "originTextID": anf.get("anforande_id") or "",
            "sourceURI": anf.get("protokoll_url_www") or "",
            "creator": SPEECH_CREATOR,
            "license": SPEECH_LICENSE,
            "textBody": text_body,
        }],
        "debug": {
            "anforandeNummer": anf.get("anforande_nummer") or "",
        },
    }


def parse_bundle(bundle: dict, spacy_model: str) -> dict:
    """Convert a scraper bundle into the ``{meta, data}`` proceedings shape."""
    import spacy
    logger.info(f"Loading spaCy model {spacy_model}")
    nlp = spacy.load(spacy_model)

    prot = bundle["protokoll"]
    anforanden = bundle["anforanden"]
    rm = prot["rm"]
    period_number = int(rm.split("/")[0])
    session_number = int(prot["nummer"])
    session_str = f"{period_number}-{session_number:03d}"

    speeches: list[dict] = []
    for anf in anforanden:
        # Use anforande_nummer (1..N) as both the source ordering signal and
        # the Stage-2 speechIndex. The bundle preserves ascending order from
        # the scraper's walk.
        try:
            anf_nr = int(anf.get("anforande_nummer") or 0)
        except (TypeError, ValueError):
            anf_nr = len(speeches) + 1
        if anf_nr <= 0:
            anf_nr = len(speeches) + 1
        speeches.append(speech_record(anf, prot, anf_nr, nlp))

    date = parse_protokoll_date(prot)
    return {
        "meta": {
            "session": session_str,
            "processing": {
                "parse_proceedings": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
            "dateStart": f"{date.isoformat()}T00:00:00",
            "dateEnd": f"{date.isoformat()}T23:59:59",
        },
        "data": speeches,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path,
                        help="OpenParliamentTV-Data-SE root directory")
    parser.add_argument("--session", required=True,
                        help="Session string (e.g. 2025-091)")
    parser.add_argument("--spacy-model", default=None,
                        help="Override the spaCy model (default: from manifest)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spacy_model = args.spacy_model
    if spacy_model is None:
        from optv.parliaments import get_locale
        spacy_model = get_locale("SE")["spacy_model"]

    config = Config(args.data_dir)
    bundle_path = config.dir("proceedings") / f"{args.session}-anforanden.json"
    if not bundle_path.exists():
        sys.exit(f"Bundle not found: {bundle_path}")

    bundle = json.loads(bundle_path.read_text())
    doc = parse_bundle(bundle, spacy_model)
    out = config.file(args.session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} speeches)")


if __name__ == "__main__":
    main()
