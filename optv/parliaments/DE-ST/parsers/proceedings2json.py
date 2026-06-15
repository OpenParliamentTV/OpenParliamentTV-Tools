#! /usr/bin/env python3
"""Parse one Sitzungsperiode HTML page into per-Sitzung intermediate JSONs.

The portal groups all sittings of a Sitzungsperiode into a single HTML page
(``sp-NNN.html``) with one top-level ``<section id="section-N">`` per day.
We split by day-section, look up each section's Landtagssitzung number from
the cumulative ``sitzung-map.json``, and emit one per-day proceedings file
(``{08NNN}-proceedings.json``) carrying speaker, party, role, TOP, and the
transcript-id + cHash needed for the merger to fetch the spoken text.

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

from ..scraper.fetch_archive import load_sitzung_map, session_id
from .common import normalize_ws, parse_speaker_label, role_to_context, strip_honorifics
from optv.parliaments import get_rights as _get_rights
from optv.parliaments import get_language as _get_language

logger = logging.getLogger(__name__)

PROCEEDINGS_LICENSE = _get_rights("DE-ST", stream="proceedings")["license"]
PROCEEDINGS_LANGUAGE = _get_language("DE-ST")


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
_TRANSCRIPT_CHASH_RE = re.compile(r"cHash=([0-9a-f]+)")


def _extract_transcript_link(li_element) -> dict:
    """Find the standalone-transcript link and pull speaker-id + cHash."""
    for a in li_element.xpath('.//a[contains(@href, "transcript")]'):
        href = a.get("href") or ""
        sm = _TRANSCRIPT_SPEAKER_RE.search(href)
        ch = _TRANSCRIPT_CHASH_RE.search(href)
        if sm and ch:
            return {
                "transcript_speaker_id": sm.group(1),
                "transcript_cHash": ch.group(1),
                "transcript_href": href,
            }
    return {"transcript_speaker_id": "", "transcript_cHash": "", "transcript_href": ""}


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


def parse_speech_block(li_element, *, block_index: int) -> dict | None:
    pids = _extract_player_ids(li_element)
    if not pids["std_player_id"]:
        # No standard video → not a real speech (rare malformed wrapper).
        return None
    h3 = _h3_text(li_element)
    speaker = parse_speaker_label(h3)
    top = _extract_top_for_block(li_element)
    transcript = _extract_transcript_link(li_element)
    return {
        "block_index": block_index,
        "h3": h3,
        "speaker_label": speaker["label"],
        "party": speaker["party"],
        "role": speaker["role"],
        "is_procedural": speaker["is_procedural"],
        **pids,
        **transcript,
        **top,
    }


def parse_day_section(section_element) -> list[dict]:
    """Parse all video-list-items under one day-section, in DOM order."""
    blocks = []
    for li in section_element.xpath('.//li[contains(@class, "video-list-item")]'):
        b = parse_speech_block(li, block_index=len(blocks))
        if b is not None:
            blocks.append(b)
    return blocks


def _build_intermediate(blocks: list[dict], session_key: str, period: int,
                        sitzung: int, date: str, sp: int,
                        source_url: str) -> dict:
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
            "session": {
                "number": int(sitzung),
                "dateStart": f"{date}T00:00:00",
                "dateEnd": f"{date}T23:59:59",
            },
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
                "transcriptSpeakerId": b["transcript_speaker_id"],
                "transcriptCHash": b["transcript_cHash"],
                "isProceduralLabel": b["is_procedural"],
            },
        })

    return {
        "meta": {
            "session": session_key,
            "sitzungsperiode": int(sp),
            "processing": {
                "parse_proceedings": datetime.now().isoformat("T", "seconds"),
            },
            "dateStart": f"{date}T00:00:00",
            "dateEnd": f"{date}T23:59:59",
            "sourceURI": source_url,
        },
        "data": speeches,
    }


def parse_sp_page(sp_path: Path, sitzung_map: dict, period: int) -> dict[str, dict]:
    """Parse one ``sp-NNN.html`` and split it into per-Sitzung intermediates."""
    sp_match = re.match(r"sp-(\d+)\.html$", sp_path.name)
    if not sp_match:
        logger.warning(f"Unexpected proceedings filename: {sp_path.name}")
        return {}
    sp_number = int(sp_match.group(1))
    sp_entry = next((e for e in sitzung_map.get("sitzungsperioden", [])
                     if e["sp"] == sp_number), None)
    if not sp_entry:
        logger.warning(f"SP {sp_number} not in sitzung map")
        return {}

    tree = lxml_html.fromstring(sp_path.read_text(encoding="utf-8", errors="replace"))
    source_url = f"https://www.landtag.sachsen-anhalt.de/{sp_number}-sitzungsperiode"

    out: dict[str, dict] = {}
    for sit in sp_entry["sittings"]:
        section_idx = sit["section"]
        sections = tree.xpath(f'//section[@id="section-{section_idx}"]')
        if not sections:
            logger.warning(f"SP {sp_number}: section-{section_idx} not found in HTML")
            continue
        blocks = parse_day_section(sections[0])
        session_key = session_id(period, sit["sitzung"])
        out[session_key] = _build_intermediate(
            blocks=blocks,
            session_key=session_key,
            period=period,
            sitzung=sit["sitzung"],
            date=sit["date"],
            sp=sp_number,
            source_url=source_url,
        )
        logger.info(f"Parsed SP {sp_number} section-{section_idx} → "
                    f"{session_key}: {len(blocks)} speeches")
    return out


def parse_proceedings_directory(proceedings_dir: Path, *, media_dir: Path,
                                metadata_dir: Path) -> None:
    """Walk all sp-*.html files and produce per-Sitzung proceedings JSON."""
    proceedings_dir = Path(proceedings_dir)
    metadata_dir = Path(metadata_dir)
    map_path = metadata_dir / "sitzung-map.json"
    try:
        sitzung_map = load_sitzung_map(metadata_dir)
    except FileNotFoundError:
        logger.warning(f"No sitzung-map.json in {metadata_dir} — run --download-original first")
        return
    period = int(sitzung_map.get("period", 8))
    map_mtime = map_path.stat().st_mtime if map_path.exists() else 0

    for sp_path in sorted(proceedings_dir.glob("sp-*.html")):
        sp_match = re.match(r"sp-(\d+)\.html$", sp_path.name)
        if not sp_match:
            continue
        sp_number = int(sp_match.group(1))
        sp_entry = next((e for e in sitzung_map.get("sitzungsperioden", [])
                         if e["sp"] == sp_number), None)
        if not sp_entry:
            continue
        outputs = [
            proceedings_dir / f"{session_id(period, sit['sitzung'])}-proceedings.json"
            for sit in sp_entry["sittings"]
        ]
        # Skip only if every expected output exists AND is newer than both the
        # SP HTML and the sitzung-map.json (the latter invalidates stale
        # per-Sitzung files whenever the Sitzung→SP assignment changes).
        if outputs and all(
            o.exists()
            and o.stat().st_mtime >= sp_path.stat().st_mtime
            and o.stat().st_mtime >= map_mtime
            for o in outputs
        ):
            continue
        parsed = parse_sp_page(sp_path, sitzung_map, period)
        for session_key, doc in parsed.items():
            out_path = proceedings_dir / f"{session_key}-proceedings.json"
            with out_path.open("w") as f:
                json.dump(doc, f, indent=2, ensure_ascii=False)
            logger.info(f"Wrote {out_path.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    parse_proceedings_directory(
        args.data_dir / "original" / "proceedings",
        media_dir=args.data_dir / "original" / "media",
        metadata_dir=args.data_dir / "metadata",
    )
