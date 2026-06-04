#! /usr/bin/env python3
"""Parse the DAR ``?sft=true`` full-text HTML into ordered speaker turns.

Input:  ``original/proceedings/{session}-dar.html`` (debates.parlamento.pt
        "Texto Completo"; fetch_proceedings.py)
Output: ``original/proceedings/{session}-proceedings.json``
        ``{"meta": {...}, "data": [<turn>, ...]}`` — one turn per speaker
        intervention, in document order. The merger matches these against the av
        intervention spine by ``matchKey`` (surname, or a chair/government role).

The full text is ``<p>`` paragraphs with inline speaker-turn markers::

    O Sr. Presidente:                      → chair, no name
    O Sr. Presidente (Teresa Morais):      → chair, name in parens (acting VP)
    O Sr. Secretário (Francisco Figueira): → officer, name in parens
    O Sr. Fabian Figueiredo (BE):          → deputy, party in parens
    A Sr.ª Mariana Mortágua (BE):          → deputy (feminine), party in parens

So the parenthetical is a **name** when the lead word is a role title, and a
**party** otherwise. Stage directions (``Aplausos``, ``Protestos``, ``Risos``)
appear in parentheses mid-text and are dropped, not treated as turns.
"""

from __future__ import annotations

import argparse
import datetime
import html as _html
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Optional

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.PT.parsers"

import lxml.etree as ET

from optv.parliaments.PT.common import Config, parse_session

logger = logging.getLogger(__name__)

PARLIAMENT_CODE = "PT"

# Speaker-turn marker: "O Sr." / "A Sr.ª" + lead + optional "(paren)" + ":".
_TURN_RE = re.compile(
    r"(?:^|\n)\s*(?P<gender>[OA])\s+Sr\.(?P<fem>ª)?\s+"
    r"(?P<lead>[^:()\n]{2,70}?)"
    r"(?:\s*\((?P<paren>[^)\n]{1,80})\))?\s*:\s",
    re.UNICODE,
)

# Lead words that are roles (the parenthetical is then a *name*, not a party).
_ROLE_LEADS = (
    "presidente", "vice presidente", "secretario", "secretaria",
    "ministro", "ministra", "secretario de estado", "secretaria de estado",
    "primeiro ministro", "primeira ministra",
)

# Paragraphs that are running headers / page furniture, not speech.
_BOILERPLATE_RE = re.compile(
    r"^(?:\d{1,4}|[IVX]+\s+S[ÉE]RIE.*|DI[ÁA]RIO DA ASSEMBLEIA.*|"
    r"\d{1,2}\s+DE\s+\w+\s+DE\s+\d{4}.*)$",
    re.IGNORECASE,
)
# Standalone stage directions ("Aplausos do BE.", "Burburinho na Sala.",
# "Protestos do PS.", "O orador foi cumprimentado…"). Short paragraphs that are
# editorial annotations, not spoken words — dropped so they neither pollute the
# verbatim text nor get aligned against the applause audio by aeneas.
_STAGE_DIR_RE = re.compile(
    r"^(?:Aplausos|Protestos?|Risos|Risadas|Vozes|Burburinho|Murm[úu]rios?|"
    r"Apartes?|Pausa|Manifesta[çc][õo]es|Vaias|Interrup[çc][ãa]o|"
    r"O orador|A oradora|Os oradores)\b.{0,180}[.…]\s*$",
    re.IGNORECASE,
)
_WS_RE = re.compile(r"\s+")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return _WS_RE.sub(" ", s.lower().replace("-", " ")).strip()


def _surname(name: str) -> str:
    """Last token of a person name, normalised — the join key for deputies."""
    n = _norm(name)
    return n.split(" ")[-1] if n else ""


def _is_role_lead(lead: str) -> Optional[str]:
    """Return a canonical role key if the lead is a role title, else None."""
    nl = _norm(lead)
    for role in _ROLE_LEADS:
        if nl == role or nl.startswith(role + " "):
            return role.split(" ")[0]  # "presidente", "secretario", "ministro"
    return None


def _extract_paragraphs(html_text: str) -> list[str]:
    """Return the ordered text of speech-bearing <p> paragraphs."""
    try:
        root = ET.fromstring(html_text, ET.HTMLParser())
    except Exception as e:  # noqa: BLE001
        logger.warning(f"HTML parse failed ({e}); falling back to regex strip")
        stripped = re.sub(r"<[^>]+>", "\n", re.sub(r"<script.*?</script>|<style.*?</style>",
                                                    " ", html_text, flags=re.S))
        return [p.strip() for p in stripped.split("\n") if p.strip()]
    paras: list[str] = []
    for p in root.iter("{*}p"):
        txt = _html.unescape(_WS_RE.sub(" ", "".join(p.itertext())).strip())
        if not txt or _BOILERPLATE_RE.match(txt) or _STAGE_DIR_RE.match(txt):
            continue
        paras.append(txt)
    return paras


class _Sentencizer:
    """spaCy Portuguese sentence segmentation with a regex fallback."""

    def __init__(self, spacy_model: Optional[str]):
        self._nlp = None
        if spacy_model:
            try:
                import spacy
                self._nlp = spacy.load(spacy_model, disable=["ner", "lemmatizer"])
                logger.info(f"loaded spaCy model {spacy_model}")
            except Exception as e:  # noqa: BLE001
                logger.warning(f"spaCy/{spacy_model} unavailable ({e}); "
                               "using regex sentence splitter")

    def __call__(self, text: str) -> list[str]:
        text = (text or "").strip()
        if not text:
            return []
        if self._nlp is not None:
            return [s.text.strip() for s in self._nlp(text).sents if s.text.strip()]
        return [s.strip() for s in re.split(r"(?<=[.!?…])\s+(?=[A-ZÀ-ÖØ-Ý])", text) if s.strip()]


def parse_dar(html_text: str, session: str, spacy_model: Optional[str]) -> dict:
    paras = _extract_paragraphs(html_text)
    blob = "\n".join(paras)
    sentencize = _Sentencizer(spacy_model)

    matches = list(_TURN_RE.finditer(blob))
    turns: list[dict] = []
    for i, m in enumerate(matches):
        lead = m.group("lead").strip()
        paren = (m.group("paren") or "").strip()
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(blob)
        text = blob[body_start:body_end].strip()

        role_key = _is_role_lead(lead)
        if role_key:
            # parenthetical (if any) is the officer's name
            name = paren or lead
            party = ""
            match_key = role_key
            is_chair = role_key in ("presidente", "secretario")
        else:
            name = lead
            party = paren
            match_key = _surname(lead)
            is_chair = False

        sentences = [{"text": s} for s in sentencize(text)]
        turns.append({
            "index": i + 1,
            "speaker": name,
            "surname": _surname(name) if not role_key else "",
            "matchKey": match_key,
            "role": lead if role_key else "",
            "party": party,
            "isChair": is_chair,
            "sentences": sentences,
        })

    logger.debug(f"[{session}] parsed {len(turns)} speaker turns from {len(paras)} paragraphs")
    return {
        "meta": {
            "session": session,
            "parliament": PARLIAMENT_CODE,
            "processing": {
                "parse_proceedings": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": turns,
    }


def parse_proceedings_for_session(config: Config, session: str,
                                  spacy_model: Optional[str]) -> dict:
    dar_path = config.raw_dar(session)
    if not dar_path.exists():
        raise FileNotFoundError(f"[{session}] DAR HTML missing: {dar_path}")
    return parse_dar(dar_path.read_text(encoding="utf-8"), session, spacy_model)


def parse_proceedings_directory(config: Config, args) -> None:
    spacy_model = getattr(args, "spacy_model", None)
    for session in config.sessions():
        if getattr(args, "pt_session", None) and session not in args.pt_session:
            continue
        if getattr(args, "limit_session", None):
            try:
                if not re.match(args.limit_session, session):
                    continue
            except re.error:
                if args.limit_session != session:
                    continue
        raw = config.raw_dar(session)
        if not raw.exists():
            logger.warning(f"[{session}] no DAR HTML — emitting no proceedings")
            continue
        out = config.file(session, "proceedings")
        if (out.exists() and not args.force
                and out.stat().st_mtime > raw.stat().st_mtime):
            logger.debug(f"[{session}] proceedings cached")
            continue
        logger.info(f"[{session}] parsing DAR text")
        doc = parse_proceedings_for_session(config, session, spacy_model)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"[{session}] wrote {out.name} ({len(doc['data'])} turns)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 17-1-059")
    parser.add_argument("--spacy-model", default=None)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spacy_model = args.spacy_model
    if spacy_model is None:
        from optv.parliaments import get_locale
        spacy_model = get_locale("PT")["spacy_model"]
    config = Config(args.data_dir)
    parse_session(args.session)
    doc = parse_proceedings_for_session(config, args.session, spacy_model)
    out = config.file(args.session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} turns)")


if __name__ == "__main__":
    main()
