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
from optv.parliaments.ES.parsers.language import split_language_passes

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
# Page anchor: <a name='(Página3)'><b>Página 3</b></a> (usually wrapped in a
# centered <p>); the visible "Página 3" label goes with it.
_PAGE_ANCHOR_RE = re.compile(
    r"<a name='\(P[aá]gina(\d+)\)'>(?:\s*<b>\s*P[aá]gina\s*\d+\s*</b>)?")
# Speaker turn: name/role must start with >=2 uppercase letters so SUMARIO
# narrative ("La señora secretaria ... da lectura") and inline mentions are
# not mistaken for turns. Optional "(Surname)" tail for chair/government roles.
_SPEAKER_RE = re.compile(
    r"(El señor|La señora)\s+([A-ZÁÉÍÓÚÑÜÇ]{2}[^:<\x00]{0,180}?):")
# Diario header: "Sesión plenaria núm. 178" or, for the Diputación
# Permanente sittings that share the Diario series, "Sesión de la Diputación
# Permanente núm. 2".
_SITTING_RE = re.compile(
    r"Sesi[oó]n (plenaria|de la Diputaci[oó]n Permanente) n[uú]m\.\s*(\d+)")
_DOC_ID_RE = re.compile(r"DSCD-\d+-PL-\d+")
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
    \\x00PAGE:N\\x00 sentinels and <br> (the paragraph separator) with
    newlines."""
    # Page anchors -> sentinel (do before tag stripping).
    inner = _PAGE_ANCHOR_RE.sub(lambda m: _PAGE_SENTINEL.format(m.group(1)), inner)
    inner = re.sub(r"<br\s*/?>", "\n", inner, flags=re.I)
    inner = _TAG_RE.sub(" ", inner)
    inner = html_lib.unescape(inner)
    inner = _WS_RE.sub(" ", inner)
    return inner


_SENTENCE_END = ".!?:)»\"…"


def _paragraphs(body: str) -> list:
    """Split a turn body into whitespace-normalised paragraphs.

    A page break sits between paragraph separators even mid-sentence, so it
    only counts as a paragraph break when the text before it ends a sentence
    and the text after it starts one.
    """
    def page_break(m: re.Match) -> str:
        before = m.string[:m.start()].rstrip()
        after = m.string[m.end():].lstrip()
        if (before and before[-1] in _SENTENCE_END
                and after and (after[0].isupper() or after[0] in "¿¡(«\"")):
            return "\n"
        return " "
    body = re.sub(r"\s*\x00PAGE:\d+\x00\s*", page_break, body)
    paragraphs = (_WS_RE.sub(" ", p).strip() for p in body.split("\n"))
    return [p for p in paragraphs if p]


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
    """Parse the HTML into the proceedings stream dict.

    `session_number` is the plenary session number; `doc_id` the Diario id
    (DSCD-<leg>-PL-<n>, the text document) the scraper recorded from the feed.
    Without it, the Diario's own `cve:` is used (minus a correction suffix
    such as "-C1").
    """
    cve, inner = extract_textointegro(html)
    cve_base = _DOC_ID_RE.match(cve or "")
    cve_base = cve_base.group(0) if cve_base else ""
    if cve_base and doc_id and cve_base != doc_id:
        logger.warning(f"Diario id {cve} differs from the expected {doc_id}")
    doc_id = doc_id or cve_base
    _check_sitting_header(_header_text(html), session_number, doc_id)
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
        gender, namepart = turn.group(1), " ".join(turn.group(2).split())
        seg_start = turn.end()
        seg_end = turns[i + 1].start() if i + 1 < len(turns) else len(text)
        paragraphs = _paragraphs(text[seg_start:seg_end])
        if not paragraphs:
            continue
        page = page_for(turn.start()) or 1
        person = parse_speaker(gender, namepart)
        origin = f"{doc_id}-{i + 1}"

        def text_content(language: str, paras: list) -> dict:
            body = " ".join(paras)
            return {
                "type": "proceedings",
                "language": language,
                "originTextID": origin,
                "sourceURI": f"{pdf_base}#page={page}",
                "creator": PROCEEDINGS_CREATOR,
                "license": PROCEEDINGS_LICENSE,
                "textBody": [{"type": "speech", "text": body,
                              "sentences": _split_sentences(body)}],
            }

        # A co-official-language turn is the original followed by its
        # Spanish version: one textContents entry each, original first.
        passes = split_language_passes(paragraphs)
        if passes:
            language, original, spanish = passes
            text_contents = [text_content(language, original),
                             text_content(PROCEEDINGS_LANGUAGE, spanish)]
        else:
            language = PROCEEDINGS_LANGUAGE
            text_contents = [text_content(PROCEEDINGS_LANGUAGE, paragraphs)]
        speeches.append({
            "parliament": PARLIAMENT,
            "electoralPeriod": {"number": period},
            "session": {"number": session_number},
            "speechIndex": PROC_INDEX_BASE + i + 1,
            "originID": origin,
            "originTextID": origin,
            "originalLanguage": language,
            "agendaItem": {"title": "Sesión plenaria", "officialTitle": "Sesión plenaria"},
            "people": [person],
            "textContents": text_contents,
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


def _header_text(html: str) -> str:
    """Flattened Diario header (cabecera) preceding the textoCompleto block."""
    start = html.find('<div class="textoIntegro">')
    if start < 0:
        return ""
    end = html.find('<p class="textoCompleto">', start)
    region = html[start:end if end > 0 else start + 5000]
    return _WS_RE.sub(" ", html_lib.unescape(_TAG_RE.sub(" ", region)))


def _check_sitting_header(header: str, session_number: int, doc_id: str) -> None:
    """Warn if the Diario header names another sitting than the session."""
    m = _SITTING_RE.search(header)
    if not m:
        logger.warning(f"{doc_id}: no sitting number in the Diario header")
    elif not m.group(1).startswith("plenaria"):
        logger.warning(f"{doc_id}: Diputación Permanente sitting {m.group(2)}, not a plenary session")
    elif int(m.group(2)) != session_number:
        logger.warning(f"{doc_id}: header gives plenary session {m.group(2)}, expected {session_number}")


def _now() -> str:
    from datetime import datetime
    return datetime.now().isoformat("T", "seconds")


def _empty_proceedings(doc_id: str, session_number: int, period: int) -> dict:
    return {"meta": {"session": f"{period}{session_number:03d}",
                     "processing": {"parse_proceedings": _now()},
                     "dateStart": None, "dateEnd": None},
            "data": []}


_FILE_RE = re.compile(r"(\d{2})(\d{3})-proceedings\.html$")


def _expected_doc_id(source: Path) -> str:
    """Diario id the scraper recorded for this session (raw media meta), else ''.

    The file name carries the plenary session number, not the Diario number.
    """
    sid = source.name[:-len("-proceedings.html")]
    raw = source.parent.parent / "media" / f"raw-{sid}-media.json"
    try:
        return json.loads(raw.read_text()).get("meta", {}).get("docId") or ""
    except (OSError, ValueError):
        return ""


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
        doc_id = _expected_doc_id(source)
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
            doc_id = _expected_doc_id(source)
            data = parse_proceedings_html(source.read_text(encoding="utf-8", errors="replace"),
                                          session_number, period, doc_id, source_uri=str(source))
            json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
