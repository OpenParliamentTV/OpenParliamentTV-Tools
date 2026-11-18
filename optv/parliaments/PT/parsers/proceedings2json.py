#! /usr/bin/env python3
"""Parse the DAR ``?sft=true`` full-text HTML into ordered speaker turns.

Input:  ``original/proceedings/{session}-dar.html`` (debates.parlamento.pt
        "Texto Completo"; fetch_proceedings.py)
Output: ``original/proceedings/{session}-proceedings.json``
        ``{"meta": {...}, "data": [<turn>, ...]}`` — one turn per speaker
        marker, in document order. The merger groups these into speeches and
        joins them onto the av intervention spine.

The full text is one ``<p>`` per PDF line inside one
``<div class="waypointMarker" data-page="N">`` per Diário page. A turn starts
at a line beginning with a speaker marker::

    O Sr. Presidente: —                        → chair, no name
    O Sr. Presidente (Teresa Morais): —        → chair, name in parens (acting VP)
    O Sr. Secretário (Francisco Figueira): —   → officer, name in parens
    O Sr. Primeiro-Ministro (Luís Montenegro): — → government, name in parens
    O Sr. Primeiro-Ministro: —                 → same office, name given earlier
    O Sr. Fabian Figueiredo (BE): —            → deputy, party in parens
    A Sr.ª Mariana Mortágua (BE): —            → deputy (feminine), party in parens

So the parenthetical is a **name** when the lead is an office, and a **party**
otherwise. An office holder is named only at the office's first marker, so the
name is carried forward per office. The automatically extracted text has marker
variants: ``Sr.a`` for ``Sr.ª``, a missing colon, a lost opening or closing
parenthesis, ``⎯`` for the em dash, and names without the space
(``FilipeSousa``).

Each turn records the Diário pages it spans (``page`` / ``pageEnd``) and
whether its text opens with an ellipsis (``continuation``: the speaker resumes
after an interruption). Interjections from the floor (*apartes*, e.g. "Muito
bem!") are turns of their own; the merger tells them apart from speeches.
Stage directions (``Aplausos do PS.``, ``Risos.``, ``Eram 15 horas…``) are
dropped.
"""

from __future__ import annotations

import argparse
import datetime
import html as _html
import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional

import lxml.etree as ET

from optv.parliaments.PT.common import Config, parse_session
from optv.shared.sentence_split import split_long_sentences

logger = logging.getLogger(__name__)

PARLIAMENT_CODE = "PT"

_DASH = "—⎯–"

# Speaker marker at the start of a line: "O Sr." / "A Sr.ª" (or "A Sr.a") + lead
# + optional parenthetical + ":" + dash. Without a parenthetical the colon is
# required, so prose like "O Sr. Ministro disse — e bem — …" is not a marker.
_MARKER_RE = re.compile(
    r"^(?P<gender>[OA])\s+Sr\.(?:ª|a)?\s+"
    rf"(?P<lead>[^:()\n{_DASH}]{{2,70}}?)\s*"
    r"(?:"
    rf"\(\s*(?P<paren>[^():\n{_DASH}]{{1,80}}?)\s*\)?\s*:?"   # (BE): / (BE) / (BE:
    r"|(?P<paren2>[A-Z][\w-]{0,12})\)\s*:?"                   # BE): (lost "(")
    r"|:"                                                      # no parenthetical
    rf")\s*[{_DASH}]\s*",
    re.UNICODE,
)

# Office leads (normalised, longest first). The chair offices map to the chair
# key; every other office is resolved to the office holder's name.
_CHAIR_LEADS = ("vice presidente", "presidente")
_OFFICE_PREFIXES = (
    "vice primeiro ministro", "vice primeira ministra",
    "primeiro ministro", "primeira ministra",
    "secretario de estado", "secretaria de estado",
    "ministro", "ministra", "secretario", "secretaria",
    "provedor", "provedora",
)
CHAIR_KEY = "presidente"

# Paragraphs that are running headers / page furniture, not speech.
_BOILERPLATE_RE = re.compile(
    r"^(?:\d{1,4}|[IVX]+\s+S[ÉE]RIE.*|DI[ÁA]RIO DA ASSEMBLEIA.*|"
    r"\d{1,2}\s+DE\s+\w+\s+DE\s+\d{4}.*)$",
    re.IGNORECASE,
)
# Standalone stage directions ("Aplausos do BE.", "Burburinho na Sala.",
# "Protestos do PS.", "O orador foi cumprimentado…", "Eram 15 horas e 3
# minutos."). Editorial annotations, not spoken words — dropped so they neither
# pollute the verbatim text nor get aligned against the applause audio.
_STAGE_DIR_RE = re.compile(
    r"^(?:Aplausos|Protestos?|Risos|Risadas|Vozes|Burburinho|Murm[úu]rios?|"
    r"Apartes?|Pausa|Manifesta[çc][õo]es|Vaias|Interrup[çc][ãa]o|"
    r"O orador|A oradora|Os oradores|Eram \d+ horas)\b.{0,180}[.…]\s*$",
    re.IGNORECASE,
)
_WS_RE = re.compile(r"\s+")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return _WS_RE.sub(" ", s.lower().replace("-", " ")).strip()


def _surname(name: str) -> str:
    """Last token of a person name, normalised — the join key for speakers."""
    n = _norm(name)
    return n.split(" ")[-1] if n else ""


def _name_key(name: str) -> str:
    """Normalised name without spaces (matches ``FilipeSousa`` to ``Filipe Sousa``)."""
    return _norm(name).replace(" ", "")


def _office(lead: str) -> Optional[str]:
    """``"chair"`` for the chair, ``"office"`` for another office, else None."""
    nl = _norm(lead)
    for prefix in _CHAIR_LEADS:
        if nl == prefix or nl.startswith(prefix + " "):
            # "Presidente da República" etc. would be a guest, not the chair;
            # only the bare chair title counts.
            return "chair" if nl == prefix else "office"
    for prefix in _OFFICE_PREFIXES:
        if nl == prefix or nl.startswith(prefix + " "):
            return "office"
    return None


def _extract_lines(html_text: str) -> list[tuple[str, int]]:
    """Return the ordered ``(text, page)`` of speech-bearing lines."""
    try:
        root = ET.fromstring(html_text, ET.HTMLParser())
    except Exception as e:  # noqa: BLE001
        logger.warning(f"HTML parse failed ({e}); falling back to regex strip")
        stripped = re.sub(r"<[^>]+>", "\n", re.sub(r"<script.*?</script>|<style.*?</style>",
                                                    " ", html_text, flags=re.S))
        return [(p.strip(), 0) for p in stripped.split("\n") if p.strip()]
    lines: list[tuple[str, int]] = []
    pages = [d for d in root.iter("{*}div")
             if "waypointMarker" in (d.get("class") or "").split()]
    containers = pages or [root]
    for div in containers:
        try:
            page = int(div.get("data-page") or 0)
        except ValueError:
            page = 0
        for p in div.iter("{*}p"):
            txt = _html.unescape(_WS_RE.sub(" ", "".join(p.itertext())).strip())
            if not txt or _BOILERPLATE_RE.match(txt) or _STAGE_DIR_RE.match(txt):
                continue
            lines.append((txt, page))
    return lines


def _join_lines(lines: list[str]) -> str:
    """Join PDF lines into running text; a line-final hyphen joins without space."""
    out = ""
    for line in lines:
        if not out:
            out = line
        elif out.endswith("-"):
            out += line
        else:
            out += " " + line
    return out.strip()


# A sentence ending in a form of address ("Sr. Presidente, Sr.as e Srs." +
# "Deputados: …") was cut after the abbreviation, not at a sentence end.
_ADDRESS_END_RE = re.compile(r"(?:^|[\s,(])(?:Sr|Srs|Sras|Dr|Drs|Ex)\.(?:ª|a|as|o|os|mo|ma)?$")


def join_split_fragments(sents: list[str]) -> list[str]:
    """Rejoin sentences that a splitter cut after a form of address
    (``Sr.`` / ``Srs.`` / ``Sr.as``), and fold punctuation-only pieces into a
    neighbour: a leading one (the ``…`` opening a resumed speech) into the
    following sentence, any later one (the ``!`` of ``?!``, a closing ``…``
    or ``»``) into the previous sentence."""
    out: list[str] = []
    pending = ""
    for s in sents:
        if not any(ch.isalnum() for ch in s):
            if out:
                sep = " " if s.startswith("—") else ""
                out[-1] = f"{out[-1]}{sep}{s}"
            else:
                pending = f"{pending} {s}".strip()
            continue
        if pending:
            s, pending = f"{pending} {s}", ""
        if out and _ADDRESS_END_RE.search(out[-1]):
            out[-1] = f"{out[-1]} {s}"
        else:
            out.append(s)
    if pending:
        out.append(pending)
    return out


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
            sents = [s.text.strip() for s in self._nlp(text).sents if s.text.strip()]
        else:
            sents = [s.strip() for s in re.split(r"(?<=[.!?…])\s+(?=[A-ZÀ-ÖØ-Ý])", text) if s.strip()]
        # Generic (punctuation-only) length-gated split of over-long sentences.
        return split_long_sentences(join_split_fragments(sents))


def parse_dar(html_text: str, session: str, spacy_model: Optional[str]) -> dict:
    lines = _extract_lines(html_text)
    sentencize = _Sentencizer(spacy_model)

    # Group lines into raw turns at each marker line.
    raw: list[dict] = []
    for txt, page in lines:
        m = _MARKER_RE.match(txt)
        if m:
            raw.append({"m": m, "lines": [txt[m.end():]], "page": page, "pageEnd": page})
        elif raw:
            raw[-1]["lines"].append(txt)
            raw[-1]["pageEnd"] = page

    office_name: dict[str, str] = {}   # normalised office lead → holder's name
    turns: list[dict] = []
    for i, r in enumerate(raw):
        m = r["m"]
        lead = _WS_RE.sub(" ", m.group("lead")).strip()
        paren = _WS_RE.sub(" ", (m.group("paren") or m.group("paren2") or "")).strip()
        text = _join_lines(r["lines"])
        office = _office(lead)
        if office == "chair":
            name = paren or "Presidente"
            party = ""
            match_key = CHAIR_KEY
        elif office == "office":
            nl = _norm(lead)
            if paren:
                office_name[nl] = paren
            name = paren or office_name.get(nl, "")
            party = ""
            # An office holder not named yet gets no key here; the merger
            # resolves it from the av intervention with the same role.
            match_key = _surname(name) if name else ""
        else:
            name = lead
            party = paren
            match_key = _surname(lead)

        sentences = [{"text": s} for s in sentencize(text)]
        turns.append({
            "index": i + 1,
            "speaker": name or lead,
            "lead": lead,
            "role": lead if office else "",
            "party": party,
            "isChair": office == "chair",
            "matchKey": match_key,
            "nameKey": _name_key(name) if name else "",
            "continuation": text.startswith("…"),
            "page": r["page"],
            "pageEnd": r["pageEnd"],
            "chars": len(text),
            "sentences": sentences,
        })

    logger.debug(f"[{session}] parsed {len(turns)} speaker turns from {len(lines)} lines")
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
