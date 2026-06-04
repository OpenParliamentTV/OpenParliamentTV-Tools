#! /usr/bin/env python3

# Convert OPAL "Suche nach Reden" rendered-search-result HTML pages into
# raw per-session media JSON files for the parser to consume.
#
# OPAL is a JS SPA wrapping Cuadra STAR; result pages must be obtained
# from a headless browser (or saved manually from a real browser). This
# scraper expects HTML files dropped into <data>/original/media/inbox/.
# Each HTML file contains 50 search results (one OPAL "page"); a single
# page usually covers many sessions (~12 speeches per session shown).
#
# We extract per-row: speaker name+party, page range, function tag, and
# the direct .mp4 URL (data-video on the play button), grouping rows by
# their <p>Plenarsitzung WP/SS</p> header.
#
# Output: <data>/original/media/raw-<session>-media.json
# Re-running merges new HTML inputs into existing per-session files.
#
# A future iteration may add a --live flag that drives OPAL via Playwright.

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import json
import logging
from lxml import html as lxml_html
from pathlib import Path
import re
import sys

logger = logging.getLogger(__name__)

VIDEO_HOST = "https://dokumente.landtag.rlp.de"
OPAL_BASE = "https://opal.rlp.de/"
# OPAL's per-speech HTML player. The `videourl` query param is the unique
# per-speech key — using this URL as `sourcePage` gives the platform a
# deep link that's stable and one-to-one with the speech.
OPAL_PLAYER = "https://opal.rlp.de/cgi-bin/ff.pl?form=video.html&videourl="

VIDEO_URL_RE = re.compile(
    r"^https?://[^/]+/landtag/opal-videos/(?P<session>\d+)-Sit(?P<index>\d+)\.mp4$"
)

# A row's text typically looks like:
#   Wansch, Thomas  (SPD) S. 26-30
#   Wansch, Thomas  (SPD) S. (Bericht) 26-30
#   Schnieder, Gordon  (CDU) S. 63-65 (Kurzintervention)
ROW_RE = re.compile(
    r"^(?P<lastname>[^,]+),\s*(?P<firstname>[^()]+?)\s*"
    r"\((?P<party>[^)]+)\)"
    r"(?:\s*S\.?\s*"
    r"(?:\((?P<func1>[^)]+)\)\s*)?"
    r"(?P<pages>[\d-]+)?"
    r"\s*(?:\((?P<func2>[^)]+)\))?\s*)?"
    r"$",
    re.UNICODE,
)


def parse_session_header(text: str) -> tuple[str, str] | None:
    """Match 'Plenarsitzung 18/77' → ('18','77')."""
    m = re.match(r"\s*Plenarsitzung\s+(\d+)/(\d+)\s*$", text or "")
    if not m:
        return None
    return m.group(1), m.group(2)


def normalize_speaker(label: str) -> tuple[str, str]:
    """'Wansch, Thomas' → ('Thomas', 'Wansch') — return (firstname, lastname)."""
    parts = [p.strip() for p in label.split(",", 1)]
    if len(parts) == 2:
        return parts[1], parts[0]
    return label, ""


def _row_text_excluding_anchor(p_el, anchor) -> str:
    """Return the text inside <p>, excluding the play-button <a>'s subtree."""
    parts: list[str] = []
    if p_el.text:
        parts.append(p_el.text)
    for child in p_el:
        if child is anchor:
            if child.tail:
                parts.append(child.tail)
            continue
        if anchor in child.iter():
            # Skip the anchor's subtree but keep tail text after it.
            for sub in child.iter():
                if sub is anchor:
                    if sub.tail:
                        parts.append(sub.tail)
                    continue
                if sub is not child and sub.text:
                    parts.append(sub.text)
                if sub is not child and sub.tail and sub is not anchor:
                    parts.append(sub.tail)
            if child.tail:
                parts.append(child.tail)
            continue
        # Whole subtree text
        for sub in child.iter():
            if sub is not child and sub.text:
                parts.append(sub.text)
            if sub.tail:
                parts.append(sub.tail)
        if child.text:
            parts.append(child.text)
        if child.tail:
            parts.append(child.tail)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def extract_rows(doc, default_period: str = "18") -> list[dict]:
    """Walk all speaker-detail rows; emit one dict per row.

    OPAL renders each speech twice — once in a tile-summary view and once
    in expandable speaker-detail rows under each session group. We target
    the speaker-detail rows (<p class="ml-5 ... border-left ..."> ...) which
    are the only complete rows: speaker, party, page range, function tag,
    video URL.

    The video URL pattern <session>-Sit<index>.mp4 carries session and
    speech-within-session index unambiguously, so we don't need to track
    the surrounding 'Plenarsitzung WP/SS' header.
    """
    root = doc.getroot() if hasattr(doc, "getroot") else doc
    rows_seen: dict[str, dict] = {}

    # XPath: paragraphs that look like speaker-detail rows (have ml-5 + border-left
    # class tokens) and contain a play-button anchor with data-video.
    speaker_ps = root.xpath(
        '//p[contains(concat(" ", normalize-space(@class), " "), " ml-5 ") '
        'and contains(concat(" ", normalize-space(@class), " "), " border-left ") '
        'and .//a[@data-video]]'
    )

    for p_el in speaker_ps:
        anchors = p_el.xpath(".//a[@data-video]")
        if not anchors:
            continue
        a = anchors[0]
        video_url = a.attrib.get("data-video") or ""
        vm = VIDEO_URL_RE.match(video_url)
        if not vm:
            logger.debug(f"Skipping non-OPAL video URL: {video_url}")
            continue
        text = _row_text_excluding_anchor(p_el, a)
        m = ROW_RE.match(text)
        if not m:
            logger.debug(f"Skipping unparseable row: {text!r}")
            continue
        firstname = m.group("firstname").strip()
        lastname = m.group("lastname").strip()
        speaker_label = f"{firstname} {lastname}"
        party = (m.group("party") or "").strip()
        pages = (m.group("pages") or "").strip()
        function = (m.group("func1") or m.group("func2") or "").strip()

        origin_media_id = f"{vm.group('session')}-Sit{vm.group('index')}"
        if origin_media_id in rows_seen:
            # Same row may be repeated across pagination snapshots; keep first.
            continue
        rows_seen[origin_media_id] = {
            "parliament": "DE-RP",
            "session_period": default_period,
            "session_number": vm.group("session"),
            "speech_index_in_session": int(vm.group("index")),
            "speaker_label": speaker_label,
            "speaker_firstname": firstname,
            "speaker_lastname": lastname,
            "faction": party,
            "page_range": pages,
            "function": function,
            "video_url": video_url,
            "origin_media_id": origin_media_id,
            "source_page": f"{OPAL_PLAYER}{video_url}",
        }
    return list(rows_seen.values())


def parse_html_file(path: Path) -> list[dict]:
    with path.open("rb") as f:
        doc = lxml_html.parse(f)
    rows = extract_rows(doc)
    logger.info(f"{path.name}: parsed {len(rows)} rows")
    return rows


def merge_into_raw_files(rows: list[dict], media_dir: Path) -> dict[str, int]:
    """Append rows to per-session raw-<session>-media.json files.

    Existing rows are deduped by origin_media_id so the same HTML can be
    re-ingested without duplication.
    """
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)

    by_session: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        sid = f"{r['session_period']}{r['session_number'].zfill(3)}"
        by_session[sid].append(r)

    counts: dict[str, int] = {}
    for sid, new_rows in by_session.items():
        path = media_dir / f"raw-{sid}-media.json"
        existing: list[dict] = []
        if path.exists():
            with path.open() as f:
                existing = json.load(f).get("rows", [])
        index = {r["origin_media_id"]: r for r in existing}
        for r in new_rows:
            index[r["origin_media_id"]] = r
        merged = sorted(index.values(), key=lambda r: r["speech_index_in_session"])
        out = {
            "meta": {
                "session": sid,
                "source": "opal-search-rendered-html",
                "ingested_at": datetime.now().isoformat("T", "seconds"),
            },
            "rows": merged,
        }
        with path.open("w") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        counts[sid] = len(merged)
    return counts


def ingest_html_inbox(inbox_dir: Path, media_dir: Path) -> dict[str, int]:
    inbox_dir = Path(inbox_dir)
    if not inbox_dir.is_dir():
        logger.warning(f"Media inbox does not exist: {inbox_dir}")
        return {}
    all_rows: list[dict] = []
    for src in sorted(inbox_dir.glob("*.html")):
        all_rows.extend(parse_html_file(src))
    return merge_into_raw_files(all_rows, media_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert saved OPAL HTML pages into raw media JSON.")
    parser.add_argument("--inbox-dir", required=True,
                        help="Directory containing saved OPAL search HTML pages")
    parser.add_argument("--media-dir", required=True,
                        help="Target directory (typically <data>/original/media)")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    counts = ingest_html_inbox(Path(args.inbox_dir), Path(args.media_dir))
    for sid, n in sorted(counts.items()):
        logger.info(f"  {sid}: {n} rows")
    logger.info("Done")
