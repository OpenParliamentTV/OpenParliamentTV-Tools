#! /usr/bin/env python3

# Parse a Congreso "texto íntegro" HTML Diario de Sesiones into the
# intermediate "proceedings" JSON stream: one record per speaker turn, with
# the verbatim text split into sentences and the page number it starts on
# (used by the merger to cross-reference the per-speech media records).
#
# The source has no per-speech markup; turns are plain-text markers
# "El señor/La señora <ROLE/SURNAME> (Surname):" inside <p class="textoCompleto">,
# with page breaks as <a name='(PáginaN)'> anchors. This mirrors the ECPC
# ParlaMint-ES build step "translate from HTML to XML with regexp".

import logging
logger = logging.getLogger(__name__)

import argparse
import html as html_lib
import json
from pathlib import Path
import re
import sys

from spacy.lang.es import Spanish
from optv.parliaments import get_rights as _get_rights
from optv.parliaments import get_language as _get_language
from optv.shared.sentence_split import split_long_sentences

# Rule-based sentencizer, loaded once per process (matches DE proceedings2json).
_nlp = Spanish()
_nlp.add_pipe("sentencizer")

PARLIAMENT = "ES"
PROCEEDINGS_LANGUAGE = _get_language("ES")
PROCEEDINGS_CREATOR = _get_rights("ES", stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights("ES", stream="proceedings")["license"]
# Proceedings speeches use a high speechIndex base to stay distinct from the
# media-side indices, mirroring the DE convention.
PROC_INDEX_BASE = 1000

_PAGE_SENTINEL = "\x00PAGE:{}\x00"
_PAGE_TOKEN_RE = re.compile(r"\x00PAGE:(\d+)\x00")
# Page anchor: <a name='(Página3)'> (sometimes wrapped in a centered <p>).
_PAGE_ANCHOR_RE = re.compile(r"<a name='\(P[aá]gina(\d+)\)'>")
# Speaker turn: name/role must start with >=2 uppercase letters so SUMARIO
# narrative ("La señora secretaria ... da lectura") and inline mentions are
# not mistaken for turns. Optional "(Surname)" tail for chair/government roles.
_SPEAKER_RE = re.compile(
    r"(El señor|La señora)\s+([A-ZÁÉÍÓÚÑÜÇ]{2}[^:<\x00]{0,180}?):")
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t ]+")


def _split_sentences(text: str) -> list:
    sents = [str(s).strip() for s in _nlp(text).sents if str(s).strip()]
    # Generic (punctuation-only) length-gated split of over-long sentences.
    return [{"text": t} for t in split_long_sentences(sents)]


def extract_textointegro(html: str) -> tuple[str, str]:
    """Return (cve_doc_id, inner_html_of_textoCompleto)."""
    start = html.find('<div class="textoIntegro">')
    if start < 0:
        return "", ""
    region = html[start:]
    cve = ""
    m = re.search(r'cve:\s*([A-Z0-9\-]+)', region)
    if m:
        cve = m.group(1).strip()
    # The verbatim + sumario live in <p class="textoCompleto"> ... (to end of div).
    pm = re.search(r'<p class="textoCompleto">(.*)', region, re.S)
    inner = pm.group(1) if pm else region
    return cve, inner


def html_to_text_with_pages(inner: str) -> str:
    """Flatten the textoCompleto HTML to text, replacing page anchors with
    \\x00PAGE:N\\x00 sentinels and <br> with spaces."""
    # Page anchors -> sentinel (do before tag stripping).
    inner = _PAGE_ANCHOR_RE.sub(lambda m: _PAGE_SENTINEL.format(m.group(1)), inner)
    inner = re.sub(r"<br\s*/?>", " ", inner, flags=re.I)
    inner = _TAG_RE.sub(" ", inner)
    inner = html_lib.unescape(inner)
    inner = _WS_RE.sub(" ", inner)
    return inner


_OUTER_ROLE_TOKENS = re.compile(
    r"^(?:PRESIDENTE|PRESIDENTA|VICEPRESIDENTE|VICEPRESIDENTA|"
    r"SECRETARIO|SECRETARIA|"
    r"MINISTRO|MINISTRA|MINISTROS|MINISTRAS|"
    r"REPRESENTANTE)\b"
)


def parse_speaker(gender: str, namepart: str) -> dict:
    """Build a proceedings person dict from a turn marker.

    The Diario uses two distinct conventions for the parenthetical tail:

    - role token outside, surname inside  →  ``SECRETARIO (Pisarello Prados):``
      ``VICEPRESIDENTE (Rodríguez Gómez de Celis):``,
      ``REPRESENTANTE DEL PARLAMENTO DE ANDALUCÍA (Martín Iglesias):``.
    - surname outside, role description inside  →
      ``NÚÑEZ FEIJÓO (candidato a la Presidencia del Gobierno):``,
      ``SÁNCHEZ PÉREZ-CASTEJÓN (presidente del Gobierno):``.

    Distinguished by whether the outer text starts with a known
    role-keyword (`_OUTER_ROLE_TOKENS`); otherwise outer is the surname
    and the parens are a descriptive role.

    Only `lastname` (the surname) is needed for the merge; context/role are
    informative. The merger keeps the richer media-side person.
    """
    namepart = namepart.strip()
    m = re.search(r"\(([^)]+)\)\s*$", namepart)
    if m:
        outer = namepart[:m.start()].strip()
        parens = m.group(1).strip()
        if _OUTER_ROLE_TOKENS.match(outer.upper()):
            surname = parens
            role = outer
        else:
            surname = outer
            role = parens
    else:
        surname = namepart.strip()
        role = ""
    role_u = role.upper()
    if "GOBIERNO" in role_u:
        context = "main-speaker"          # government member speaking
    elif "VICEPRESIDENT" in role_u:
        context = "vice-president"
    elif "PRESIDENT" in role_u:
        context = "president"             # chamber / Mesa de Edad presidency
    else:
        context = "main-speaker"
    # Canonical label: title-case the surname (media side carries the full name).
    label = surname.title() if surname.isupper() else surname
    person = {"label": label, "lastname": label, "context": context,
              "type": "memberOfParliament"}
    if role:
        person["role"] = role.title() if role.isupper() else role
    return person


def parse_proceedings_html(html: str, session_number: int, period: int,
                           doc_id: str, source_uri: str) -> dict:
    """Parse the HTML into the proceedings stream dict."""
    cve, inner = extract_textointegro(html)
    doc_id = doc_id or cve
    text = html_to_text_with_pages(inner)

    turns = list(_SPEAKER_RE.finditer(text))
    if not turns:
        logger.warning(f"No speaker turns found in {doc_id}")
        return _empty_proceedings(doc_id, session_number, period)

    # Discard everything before the first turn (ORDEN DEL DÍA + SUMARIO).
    leg = period
    pdf_base = f"https://www.congreso.es/public_oficiales/L{leg}/CONG/DS/PL/{doc_id}.PDF"

    # Precompute page transitions: list of (offset, page).
    page_marks = [(m.start(), int(m.group(1))) for m in _PAGE_TOKEN_RE.finditer(text)]

    def page_for(offset: int) -> int:
        page = 0
        for off, pg in page_marks:
            if off <= offset:
                page = pg
            else:
                break
        return page

    speeches = []
    for i, turn in enumerate(turns):
        gender, namepart = turn.group(1), turn.group(2)
        seg_start = turn.end()
        seg_end = turns[i + 1].start() if i + 1 < len(turns) else len(text)
        body = text[seg_start:seg_end]
        body = _PAGE_TOKEN_RE.sub(" ", body)
        body = _WS_RE.sub(" ", body).strip()
        if not body:
            continue
        page = page_for(turn.start()) or 1
        person = parse_speaker(gender, namepart)
        origin = f"{doc_id}-{i + 1}"
        sentences = _split_sentences(body)
        speeches.append({
            "parliament": PARLIAMENT,
            "electoralPeriod": {"number": period},
            "session": {"number": session_number},
            "speechIndex": PROC_INDEX_BASE + i + 1,
            "originID": origin,
            "originTextID": origin,
            "agendaItem": {"title": "Sesión plenaria", "officialTitle": "Sesión plenaria"},
            "people": [person],
            "textContents": [{
                "type": "proceedings",
                "language": PROCEEDINGS_LANGUAGE,
                "originTextID": origin,
                "sourceURI": f"{pdf_base}#page={page}",
                "creator": PROCEEDINGS_CREATOR,
                "license": PROCEEDINGS_LICENSE,
                "textBody": [{"type": "speech", "text": body, "sentences": sentences}],
            }],
            "documents": [],
            "debug": {"page": page},
        })

    return {
        "meta": {
            "session": f"{period}{session_number:03d}",
            "processing": {"parse_proceedings": _now()},
            "dateStart": None,
            "dateEnd": None,
        },
        "data": speeches,
    }


def _now() -> str:
    from datetime import datetime
    return datetime.now().isoformat("T", "seconds")


def _empty_proceedings(doc_id: str, session_number: int, period: int) -> dict:
    return {"meta": {"session": f"{period}{session_number:03d}",
                     "processing": {"parse_proceedings": _now()},
                     "dateStart": None, "dateEnd": None},
            "data": []}


_FILE_RE = re.compile(r"(\d{2})(\d{3})-proceedings\.html$")


def parse_proceedings_directory(directory: Path, args=None) -> None:
    """Parse *-proceedings.html into *-proceedings.json when out of date."""
    directory = Path(directory)
    for source in sorted(directory.glob("*-proceedings.html")):
        output_file = source.parent / (source.name[:-len("-proceedings.html")] + "-proceedings.json")
        if output_file.exists() and output_file.stat().st_mtime >= source.stat().st_mtime:
            continue
        m = _FILE_RE.search(source.name)
        if not m:
            logger.warning(f"Cannot derive period/session from {source.name} - skipping")
            continue
        period, session_number = int(m.group(1)), int(m.group(2))
        doc_id = f"DSCD-{period}-PL-{session_number}"
        logger.info(f"Parsing {source.name}")
        data = parse_proceedings_html(source.read_text(encoding="utf-8", errors="replace"),
                                      session_number, period, doc_id, source_uri=str(source))
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Congreso texto-integro HTML into the proceedings stream.")
    parser.add_argument("sources", type=str, nargs="*", help="HTML file(s) or a directory")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    if not args.sources:
        parser.print_help()
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    for source in args.sources:
        source = Path(source)
        if source.is_dir():
            parse_proceedings_directory(source, args)
        else:
            m = _FILE_RE.search(source.name)
            period, session_number = (int(m.group(1)), int(m.group(2))) if m else (15, 0)
            doc_id = f"DSCD-{period}-PL-{session_number}"
            data = parse_proceedings_html(source.read_text(encoding="utf-8", errors="replace"),
                                          session_number, period, doc_id, source_uri=str(source))
            json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
