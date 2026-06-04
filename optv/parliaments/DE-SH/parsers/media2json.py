#! /usr/bin/env python3
"""Parse the m7k mediathek's HTML into per-Sitzung intermediate JSON.

Two raw sources per Sitzung-day:

- ``original/media/result/wp{N}/tagung-{NNN}.html`` — full ``result.php``
  response covering one whole Tagung. May contain multiple sitting days.
- ``original/media/iframe/{YYYY-MM-DD}/{speech_id}.html`` — one per speech;
  carries the ``<video><source src="…#t=start,end">…</source></video>``
  that gives us the per-speech MP4 URL.

We group the result.php speech entries by date, look up the corresponding
iframe response for each speech's video URL, and emit one intermediate
file per Sitzung at ``original/media/{wp}{NNN}-media.json`` (NNN = the
canonical Plenarprotokoll Sitzung number, supplied via the archive map
written by ``fetch_archive``).
"""

from __future__ import annotations

import argparse
import html as htmllib
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-SH.parsers"

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-SH", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-SH", stream="media")["creator"]


# One <div class="result" id="NNNNN"> per speech. We capture the inner
# block; field-level regexes pick fields out of it.
_RESULT_BLOCK_RE = re.compile(
    r'<div\s+class="result"\s+id="(?P<id>\d+)"\s*>(?P<body>.*?)</div>\s*</a>',
    re.S | re.I,
)

_FIELD_PATTERNS: dict[str, re.Pattern] = {
    "wp":      re.compile(r'<div\s+class="wp">(?P<v>[^<]*)</div>', re.I),
    "datum":   re.compile(r'<div\s+class="datum">(?P<v>[^<]*)</div>', re.I),
    "tagung":  re.compile(r'<div\s+class="tagung">(?P<v>[^<]*)</div>', re.I),
    "top":     re.compile(r'<div\s+class="top">(?P<v>[^<]*)</div>', re.I),
    "thema":   re.compile(r'<div\s+class="thema">(?P<v>[^<]*)</div>', re.I),
    "redner":  re.compile(r'<div\s+class="redner">(?P<v>[^<]*)</div>', re.I),
    "gruppe":  re.compile(r'<div\s+class="gruppe">(?P<v>[^<]*)</div>', re.I),
    "beginn":  re.compile(r'<div\s+class="beginn">(?P<v>[^<]*)</div>', re.I),
    "ende":    re.compile(r'<div\s+class="ende">(?P<v>[^<]*)</div>', re.I),
    "dauer":   re.compile(r'<div\s+class="dauer">(?P<v>[^<]*)</div>', re.I),
}

# iframe.php response has a <video> with one or more <source> tags.
# Prefer MP4 over WebM; the fragment URI carries the per-speech window.
_IFRAME_SOURCE_RE = re.compile(
    r'<source\s+src="(?P<src>[^"]+)"\s+type="video/(?P<type>mp4|webm)"',
    re.I,
)


def _text(html_fragment: str) -> str:
    """Strip HTML, unescape entities, collapse whitespace."""
    txt = re.sub(r'<[^>]+>', '', html_fragment)
    txt = htmllib.unescape(txt)
    return re.sub(r'\s+', ' ', txt).strip()


def _parse_de_date(s: str) -> str | None:
    try:
        return datetime.strptime(s.strip(), "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def _hms_to_seconds(hms: str) -> int | None:
    """``"11:04:14"`` → 39854. ``""`` → None."""
    parts = hms.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = (int(p) for p in parts)
        return h * 3600 + m * 60 + s
    except ValueError:
        return None


def parse_result_block(block_body: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for field, pat in _FIELD_PATTERNS.items():
        m = pat.search(block_body)
        out[field] = _text(m.group("v")) if m else ""
    return out


def parse_result_html(html: str) -> list[dict]:
    """Return ``[{id, wp, datum, tagung, top, thema, redner, gruppe,
    beginn, ende, dauer}, ...]``."""
    out: list[dict] = []
    for m in _RESULT_BLOCK_RE.finditer(html):
        rec = {"id": m.group("id")}
        rec.update(parse_result_block(m.group("body")))
        out.append(rec)
    return out


def parse_iframe_html(html: str) -> dict[str, str]:
    """Return ``{"mp4": "...", "webm": "..."}`` from an iframe.php response.

    Each value is the raw ``src`` URL including the ``#t=start,end``
    fragment. Empty dict on parse failure.
    """
    sources: dict[str, str] = {}
    for m in _IFRAME_SOURCE_RE.finditer(html):
        sources[m.group("type").lower()] = htmllib.unescape(m.group("src"))
    return sources


def _absolute(url: str, m7k_base: str = "https://m7k.ltsh.de") -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return m7k_base + url
    return f"{m7k_base}/{url}"


def _load_archive_index(media_dir: Path) -> dict:
    """Find ``../../metadata/archive-wp{N}.json`` from ``media_dir``."""
    metadata_dir = media_dir.parent.parent / "metadata"
    archives = sorted(metadata_dir.glob("archive-wp*.json"))
    out = {"sitzung_by_date": {}, "wp": None}
    for path in archives:
        with path.open() as f:
            arc = json.load(f)
        # Multiple WPs would merge; last write wins on date collision but
        # the period prefix in session_id keeps them distinguishable.
        out["sitzung_by_date"].update(arc.get("sitzung_by_date") or {})
        out["wp"] = arc.get("wp")
    return out


def _build_speech_record(rec: dict, iframe_dir: Path, sitzung_no: int | None,
                         wp: int) -> dict | None:
    iso = _parse_de_date(rec.get("datum", ""))
    if not iso:
        logger.debug(f"speech {rec.get('id')}: unparsable datum {rec.get('datum')!r}")
        return None

    iframe_path = iframe_dir / iso / f"{rec['id']}.html"
    sources: dict[str, str] = {}
    if iframe_path.exists():
        sources = parse_iframe_html(iframe_path.read_text(encoding="utf-8", errors="replace"))
    mp4 = sources.get("mp4")
    webm = sources.get("webm")
    video_uri = _absolute(mp4) if mp4 else (_absolute(webm) if webm else "")

    beginn_s = _hms_to_seconds(rec.get("beginn", ""))
    ende_s = _hms_to_seconds(rec.get("ende", ""))
    try:
        dauer_int = int(rec.get("dauer", "").split(":")[0]) * 3600 + \
                    int(rec.get("dauer", "").split(":")[1]) * 60 + \
                    int(rec.get("dauer", "").split(":")[2]) if rec.get("dauer") else 0
    except (ValueError, IndexError):
        dauer_int = 0

    return {
        "speech_id": rec["id"],
        "date": iso,
        "wp": rec.get("wp") or str(wp),
        "tagung_no": int(rec["tagung"]) if rec.get("tagung", "").isdigit() else None,
        "sitzung_no": sitzung_no,
        "top": rec.get("top") or None,
        "thema": rec.get("thema") or "",
        "redner": rec.get("redner") or "",
        "gruppe": rec.get("gruppe") or "",
        "beginn": rec.get("beginn") or "",
        "ende": rec.get("ende") or "",
        "beginn_seconds": beginn_s,
        "ende_seconds": ende_s,
        "duration_seconds": dauer_int,
        "videoFileURI": video_uri,
        "videoFileURI_webm": _absolute(webm) if webm else "",
        "iframe_path": str(iframe_path) if iframe_path.exists() else "",
    }


def parse_media_directory(media_dir: Path) -> None:
    """Walk ``original/media/`` and emit ``{wp}{NNN}-media.json`` per Sitzung.

    Resolves ``Sitzung`` numbers from the archive index written by
    ``fetch_archive``. Days without a Sitzung number are skipped (current
    Tagung whose PDF is not yet published).
    """
    media_dir = Path(media_dir)
    result_dir = media_dir / "result"
    iframe_dir = media_dir / "iframe"
    if not result_dir.is_dir():
        logger.warning(f"No result/ subdir under {media_dir} — nothing to parse.")
        return

    archive_index = _load_archive_index(media_dir)
    sitzung_by_date = archive_index["sitzung_by_date"]

    # Bucket parsed records by ISO date.
    speeches_by_date: dict[str, list[dict]] = {}
    skipped_chair_slots = 0
    for html_path in sorted(result_dir.rglob("tagung-*.html")):
        html = html_path.read_text(encoding="utf-8", errors="replace")
        wp_int_from_path = _extract_wp_from_dir(html_path.parent.name) \
                           or archive_index["wp"] or 20
        for rec in parse_result_html(html):
            iso = _parse_de_date(rec.get("datum", ""))
            if not iso:
                continue
            sit_no = sitzung_by_date.get(iso)
            if sit_no is None:
                logger.debug(
                    f"Skipping speech {rec.get('id')} ({iso}): no Sitzung number "
                    f"in archive index (probably a Tagung whose PDF is not yet published)."
                )
                continue
            # m7k publishes structural placeholders (thema="Präsidium",
            # "3 Minuten Beiträge", break markers) with empty redner and
            # gruppe and no associated video. They are not speeches and
            # do not have a <source> in their iframe — drop them.
            if not rec.get("redner") and not rec.get("gruppe"):
                skipped_chair_slots += 1
                continue
            speech_rec = _build_speech_record(rec, iframe_dir, sit_no, wp_int_from_path)
            if speech_rec is None:
                continue
            speeches_by_date.setdefault(iso, []).append(speech_rec)
    if skipped_chair_slots:
        logger.info(f"Skipped {skipped_chair_slots} structural placeholder entries "
                    f"(chair time / breaks with no redner+gruppe)")

    # Emit one file per Sitzung.
    for iso, speeches in speeches_by_date.items():
        sit_no = sitzung_by_date[iso]
        wp = speeches[0]["wp"] if speeches else "20"
        session_id = f"{int(wp):02d}{sit_no:03d}"
        # Sort by beginn_seconds for deterministic speechIndex assignment.
        speeches.sort(key=lambda s: (s.get("beginn_seconds") or 0, s["speech_id"]))
        for idx, s in enumerate(speeches, start=1):
            s["speech_index"] = idx
        doc = {
            "meta": {
                "session": session_id,
                "wp": int(wp),
                "date": iso,
                "tagung": speeches[0].get("tagung_no"),
                "sitzung": sit_no,
                "processing": {
                    "parse_media": datetime.now().isoformat("T", "seconds"),
                },
            },
            "data": speeches,
        }
        out_path = Path(media_dir) / f"{session_id}-media.json"
        out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"Wrote {out_path.name} ({len(speeches)} speeches, {iso})")


def _extract_wp_from_dir(dirname: str) -> int | None:
    m = re.match(r'wp(\d+)$', dirname, re.I)
    return int(m.group(1)) if m else None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    parse_media_directory(args.data_dir / "original" / "media")
