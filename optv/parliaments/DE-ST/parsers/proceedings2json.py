#! /usr/bin/env python3
"""Parse one Sitzungsperiode HTML page into per-Sitzung intermediate JSONs.

The portal groups all sittings of a Sitzungsperiode into a single HTML page
(``sp-{PP}-{NNN}.html``) with one top-level ``<section id="section-N">`` per
day. We split by day-section, look up each section's Landtagssitzung number
from ``sitzung-map-{PP}.json``, and emit one per-day proceedings file
(``{PPNNN}-proceedings.json``) carrying speaker, party, role, TOP, the clip's
page anchor and the transcript URLs the merger fetches the spoken text from.

When the sitting's protocol PDF (``{PPNNN}.pdf``) is on disk, its clock times
(start, breaks, end) go into ``meta.sittingTimes``, each break with a text
anchor that lets the merger find the clip the sitting resumed with.

Per-speech join key: the standard video player-id. The merger reads the
corresponding media JSON keyed by the same player-id and zips 1:1.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from lxml import html as lxml_html

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-ST.parsers"

from optv.shared.agenda_types import classify_de_st

from ..scraper.fetch_archive import (
    _map_path, _sp_page_path, absolute_url, load_sitzung_map, session_id,
)
from ..scraper.fetch_protocols import protocol_path
from .common import normalize_ws, parse_speaker_label, role_to_context, strip_honorifics

logger = logging.getLogger(__name__)


def _extract_top_for_block(li_element) -> dict:
    """Walk up from a <li.video-list-item> to find the enclosing TOP info.

    The DOM shape is::

        <li class="live">
          <a class="accordion no-js-build-vid" href="#section-inner-tops...">
            <div class="wrapper"><span>TOP 1</span></div>
            <h3 class="topic">Wahl des Ministerpräsidenten</h3>
          </a>
          <section id="section-inner-topsNN-NNNN" class="accordion-plenar">
            <ul class="video-list">
              <li class="video-list-item">...</li>
              ...
            </ul>
          </section>
        </li>

    So we walk up to the ``<section class="accordion-plenar">``, then to its
    sibling ``<a>`` carrying the TOP number + topic title.
    """
    p = li_element
    while p is not None:
        if p.tag == "section":
            cls = p.get("class") or ""
            if "accordion-plenar" in cls:
                break
        p = p.getparent()
    if p is None:
        return {"top_number": "", "top_title": ""}
    parent_a = p.getprevious()
    while parent_a is not None and parent_a.tag != "a":
        parent_a = parent_a.getprevious()
    if parent_a is None:
        return {"top_number": "", "top_title": ""}
    top_spans = parent_a.xpath('.//span')
    top_topic = parent_a.xpath('.//h3[@class="topic"]')
    top_number = ""
    for sp in top_spans:
        txt = (sp.text or "").strip()
        if txt.startswith("TOP"):
            top_number = txt.replace("TOP", "").strip()
            break
    top_title = normalize_ws(top_topic[0].text_content()) if top_topic else ""
    return {"top_number": top_number, "top_title": top_title}


_TRANSCRIPT_SPEAKER_RE = re.compile(r"speaker%5D=(\d+)")


def _extract_transcript_links(li_element) -> dict:
    """The clip's transcript URLs, exactly as the page links them.

    ``transcript_url`` is the AJAX fragment the player loads (its cHash is
    bound to the SP page's path, so it must not be rebuilt);
    ``transcript_page_url`` is the stand-alone transcript page.
    """
    ajax = page = ""
    for a in li_element.xpath('.//a[contains(@href, "transcript")]'):
        href = a.get("href") or ""
        if "transcriptSessions=" in href and not ajax:
            ajax = absolute_url(href)
        elif "/plenarsitzungen/transkript" in href and not page:
            page = absolute_url(href)
    m = _TRANSCRIPT_SPEAKER_RE.search(ajax or page)
    return {
        "transcript_speaker_id": m.group(1) if m else "",
        "transcript_url": ajax,
        "transcript_page_url": page,
    }


def _extract_player_ids(li_element) -> dict:
    std = None
    sign = None
    for a in li_element.xpath('.//a[@data-js-id]'):
        js = a.get("data-js-id")
        pid = a.get("data-player-id")
        if js == "video-std":
            std = pid
        elif js == "video-sign":
            sign = pid
    return {"std_player_id": std or "", "sign_player_id": sign or ""}


def _h3_text(li_element) -> str:
    h3 = li_element.xpath('.//h3[@class="no-style"]')
    return normalize_ws(h3[0].text_content()) if h3 else ""


def _video_anchor(li_element) -> str:
    """The clip's own page anchor (``#section-video-{item}-{slot}``)."""
    for a in li_element.xpath('.//a[contains(concat(" ", @class, " "), " a-video ")]'):
        href = a.get("href") or ""
        if href.startswith("#section-video-"):
            return href
    return ""


def parse_speech_block(li_element, *, block_index: int) -> dict | None:
    pids = _extract_player_ids(li_element)
    if not pids["std_player_id"]:
        # No standard video → not a real speech (rare malformed wrapper).
        return None
    h3 = _h3_text(li_element)
    has_profile = bool(li_element.xpath('.//a[contains(concat(" ", @class, " "), " profile ")]'))
    speaker = parse_speaker_label(h3, has_profile=has_profile)
    return {
        "block_index": block_index,
        "h3": h3,
        "speaker_label": speaker["label"],
        "party": speaker["party"],
        "role": speaker["role"],
        "is_procedural": speaker["is_procedural"],
        "video_anchor": _video_anchor(li_element),
        **pids,
        **_extract_transcript_links(li_element),
        **_extract_top_for_block(li_element),
    }


def parse_day_section(section_element) -> list[dict]:
    """Parse all video-list-items under one day-section, in DOM order."""
    blocks = []
    for li in section_element.xpath('.//li[contains(@class, "video-list-item")]'):
        b = parse_speech_block(li, block_index=len(blocks))
        if b is not None:
            blocks.append(b)
    return blocks


# ---------------------------------------------------------------------------
# Sitting times from the protocol
# ---------------------------------------------------------------------------

_PAGE_HEADER_RE = re.compile(
    r"LANDTAG VON SACHSEN-ANHALT\s*\|\s*\d{2}\.\d{2}\.\d{4}\s*\|\s*"
    r"Stenografischer Bericht\s*\d+/\d+(?:\s+\d+)*")
_RESUME_RE = re.compile(r"Wiederbeginn:\s*(\d{1,2})(?:[:.](\d{2}))?\s*Uhr\.?")
_SPEAKER_PREFIX_RE = re.compile(
    r"^\s*(?:(?:Alters|Vize)?[Pp]räsident(?:in)?\s+[^():]{3,80}?"
    r"|[^():]{3,80}?(?:\s*\([^()]{1,160}\))+)\s*:\s*")
ANCHOR_LETTERS = 60


def letters(text: str) -> str:
    """Lower-case letters only: comparable across PDF hyphenation and HTML."""
    return re.sub(r"[^a-zäöüß]", "", (text or "").lower())


def resume_anchors(text: str) -> dict[str, str]:
    """``{resume "HH:MM": anchor}`` for every ``Wiederbeginn:`` in the protocol.

    The anchor is the first ``ANCHOR_LETTERS`` letters of the contribution
    after the resumption's speaker line; the transcript of the clip the
    sitting resumed with contains the same text.
    """
    text = _PAGE_HEADER_RE.sub(" ", re.sub(r"\s+", " ", text or ""))
    out: dict[str, str] = {}
    for m in _RESUME_RE.finditer(text):
        rest = text[m.end():m.end() + 800]
        rest = _SPEAKER_PREFIX_RE.sub("", rest, count=1)
        anchor = letters(rest)[:ANCHOR_LETTERS]
        if len(anchor) == ANCHOR_LETTERS:
            out[f"{int(m.group(1)):02d}:{int(m.group(2) or 0):02d}"] = anchor
    return out


def sitting_times_from_text(text: str) -> dict | None:
    """Start, end and breaks (with resume anchors) from protocol text."""
    from optv.shared.pdf2tei.sitting_times import parse_sitting_times
    times = parse_sitting_times(text)
    if times is None:
        return None
    anchors = resume_anchors(text)
    return {
        "begin": times["begin"],
        "end": times["end"],
        "breaks": [{"stop": stop, "resume": resume, "anchor": anchors.get(resume)}
                   for stop, resume in times["breaks"]],
    }


def protocol_text_from_pdf(pdf_path: Path) -> str | None:
    """The protocol's text in reading order, or ``None`` for an unreadable PDF.

    The protocol body is set in two columns. Blocks are read left column
    first, then right, each top to bottom, on every page: a page whose right
    column is short (the last page) would otherwise interleave the columns.
    """
    import fitz
    try:
        doc = fitz.open(str(pdf_path))
        parts: list[str] = []
        for page in doc:
            center = page.rect.width / 2
            blocks = [b for b in page.get_text("blocks") if b[4].strip()]
            blocks.sort(key=lambda b: (b[0] >= center, b[1]))
            parts.extend(b[4] for b in blocks)
    except Exception as e:  # unreadable PDF: no times, the merge falls back
        logger.warning(f"{pdf_path}: could not read the protocol ({e})")
        return None
    return " ".join(parts)


def protocol_body_letters(text: str) -> str:
    """Letters of the protocol body (from ``Beginn:``), page headers removed.

    The table of contents before ``Beginn:`` repeats agenda titles and
    speaker names, so it is left out.
    """
    text = _PAGE_HEADER_RE.sub(" ", re.sub(r"\s+", " ", text or ""))
    start = text.find("Beginn:")
    return letters(text[start:] if start >= 0 else text)


def sitting_times_from_pdf(pdf_path: Path) -> dict | None:
    text = protocol_text_from_pdf(pdf_path)
    if text is None:
        return None
    times = sitting_times_from_text(text)
    if times is None:
        logger.warning(f"{pdf_path}: no sitting start/end found")
    return times


# ---------------------------------------------------------------------------
# Intermediate JSON
# ---------------------------------------------------------------------------

def _build_intermediate(blocks: list[dict], session_key: str, period: int,
                        sitzung: int, date: str, sp: int, sp_url: str,
                        sitting_times: dict | None = None,
                        protocol_url: str | None = None) -> dict:
    """Turn raw blocks into per-Sitzung intermediate JSON.

    Speech text is NOT populated here; the merger fetches transcripts and
    fills ``textContents[].textBody`` after the per-speech join.
    """
    speeches = []
    for idx, b in enumerate(blocks, start=1):
        speech_id = f"{session_key}-{idx:03d}"
        person: dict = {
            "type": "memberOfParliament",
            "label": b["speaker_label"] or "",
            "context": role_to_context(b["role"]) or "main-speaker",
        }
        if b["speaker_label"]:
            parts = strip_honorifics(b["speaker_label"]).split()
            if parts:
                person["firstname"] = parts[0]
                person["lastname"] = " ".join(parts[1:]) if len(parts) > 1 else ""
        if b["party"]:
            person["faction"] = {"label": b["party"]}
        if b["role"]:
            person["role"] = b["role"]

        title = b["top_title"] or b["h3"]
        native_type, core_type = classify_de_st(title)
        agenda_item = {"officialTitle": title, "title": title, "type": core_type}
        if native_type:
            agenda_item["nativeType"] = native_type
        if b["top_number"]:
            agenda_item["originID"] = f"TOP {b['top_number']}"

        speeches.append({
            "parliament": "DE-ST",
            "electoralPeriod": {"number": int(period)},
            "session": {"number": int(sitzung)},
            "speechIndex": idx,
            "originID": speech_id,
            "originTextID": speech_id,
            "agendaItem": agenda_item,
            "people": [person],
            "textContents": [],  # filled by merger after transcript fetch
            "documents": [],
            "debug": {
                "proceedingsSource": "landtag-lsa-html",
                "blockIndex": b["block_index"],
                "h3Label": b["h3"],
                "stdPlayerId": b["std_player_id"],
                "signPlayerId": b["sign_player_id"],
                "videoAnchor": b["video_anchor"],
                "transcriptSpeakerId": b["transcript_speaker_id"],
                "transcriptURL": b["transcript_url"],
                "transcriptPageURL": b["transcript_page_url"],
                "isProceduralLabel": b["is_procedural"],
            },
        })

    meta = {
        "session": session_key,
        "sitzungsperiode": int(sp),
        "processing": {
            "parse_proceedings": datetime.now().isoformat("T", "seconds"),
        },
        "date": date,
        "sourceURI": sp_url,
    }
    if protocol_url:
        meta["protocolURL"] = protocol_url
    if sitting_times:
        meta["sittingTimes"] = sitting_times
    return {"meta": meta, "data": speeches}


def parse_sp_page(sp_path: Path, sp_entry: dict, period: int,
                  proceedings_dir: Path | None = None) -> dict[str, dict]:
    """Parse one SP page and split it into per-Sitzung intermediates."""
    sp_number = int(sp_entry["sp"])
    tree = lxml_html.fromstring(Path(sp_path).read_text(encoding="utf-8", errors="replace"))

    out: dict[str, dict] = {}
    for sit in sp_entry["sittings"]:
        section_idx = sit["section"]
        sections = tree.xpath(f'//section[@id="section-{section_idx}"]')
        if not sections:
            logger.warning(f"SP {sp_number}: section-{section_idx} not found in HTML")
            continue
        blocks = parse_day_section(sections[0])
        session_key = session_id(period, sit["sitzung"])
        pdf = protocol_path(proceedings_dir, session_key) if proceedings_dir else None
        out[session_key] = _build_intermediate(
            blocks=blocks,
            session_key=session_key,
            period=period,
            sitzung=sit["sitzung"],
            date=sit["date"],
            sp=sp_number,
            sp_url=sp_entry["url"],
            sitting_times=sitting_times_from_pdf(pdf) if pdf and pdf.exists() else None,
            protocol_url=sit.get("protocolURL"),
        )
        logger.info(f"Parsed SP {sp_number} section-{section_idx} → "
                    f"{session_key}: {len(blocks)} speeches")
    return out


def _content(doc: dict) -> dict:
    """The document without its processing timestamps."""
    meta = {k: v for k, v in doc.get("meta", {}).items() if k != "processing"}
    return {"meta": meta, "data": doc.get("data")}


def write_if_changed(doc: dict, path: Path) -> bool:
    """Write ``doc`` unless the file already holds the same content.

    Unlike ``save_if_changed`` this compares ``meta`` too (minus the
    processing timestamps), so newly added sitting times are written.
    """
    if path.exists():
        try:
            if _content(json.loads(path.read_text())) == _content(doc):
                return False
        except json.JSONDecodeError:
            pass
    with path.open("w") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
    return True


def parse_proceedings_directory(proceedings_dir: Path, *, metadata_dir: Path,
                                period: int) -> None:
    """Produce per-Sitzung proceedings JSON for every SP page of ``period``."""
    proceedings_dir = Path(proceedings_dir)
    map_path = _map_path(metadata_dir, period)
    if not map_path.exists():
        logger.warning(f"No {map_path.name} in {metadata_dir} — run --download-original first")
        return
    sitzung_map = load_sitzung_map(metadata_dir, period)
    period = int(sitzung_map["period"])
    map_mtime = map_path.stat().st_mtime

    for sp_entry in sitzung_map.get("sitzungsperioden", []):
        sp_path = _sp_page_path(proceedings_dir, period, sp_entry["sp"])
        if not sp_path.exists():
            logger.warning(f"SP {sp_entry['sp']}: page HTML missing at {sp_path}")
            continue
        sessions = [session_id(period, sit["sitzung"]) for sit in sp_entry["sittings"]]
        inputs = [sp_path.stat().st_mtime, map_mtime] + [
            protocol_path(proceedings_dir, s).stat().st_mtime
            for s in sessions if protocol_path(proceedings_dir, s).exists()]
        outputs = [proceedings_dir / f"{s}-proceedings.json" for s in sessions]
        # Skip when every output is newer than the page, the map and the
        # protocols; a re-parse writes only outputs whose content changed.
        if outputs and all(o.exists() and o.stat().st_mtime >= max(inputs) for o in outputs):
            continue
        for session_key, doc in parse_sp_page(sp_path, sp_entry, period, proceedings_dir).items():
            out_path = proceedings_dir / f"{session_key}-proceedings.json"
            if write_if_changed(doc, out_path):
                logger.info(f"Wrote {out_path.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=9)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    parse_proceedings_directory(
        args.data_dir / "original" / "proceedings",
        metadata_dir=args.data_dir / "metadata",
        period=args.period,
    )
