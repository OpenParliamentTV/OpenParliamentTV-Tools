#! /usr/bin/env python3
"""Parse the downloaded DE-BW TOP manifests into per-Sitzung intermediate JSON.

Input per session (written by the scraper):

- ``original/media/{session_id}-tops.json`` — one or more ``parts`` (sequential
  video files for the same calendar-day Sitzung), each with its MP4 URL, page
  URL and ``e-chapterList`` structure: per-TOP title/description and, per
  speech, the raw speaker name (``Lastname Firstname``), a ``| Role | Faction``
  meta string and a ``changeTimestamp`` start offset (seconds into *that part's*
  MP4).

This pass: splits the name into ``Firstname Lastname`` (for NEL matching) +
first/last; splits the meta string into role + faction; computes per-speech
``end_offset`` from the next speech's start **within the same part** (the source
has no per-speech end; offsets reset per part); carries each speech's part MP4 /
page URL; and assigns a deterministic global ``speech_index`` ordered by
(part, offset). Synthetic ``start_datetime`` uses a per-part cumulative base so
the times stay globally monotonic across parts (the offsets themselves remain
part-relative, for the media fragment). One record per speech is emitted to
``original/media/{session_id}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-BW.parsers"

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-BW", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-BW", stream="media")["creator"]

# Faction shortcodes as they appear in the chapter-list meta string. Used to
# tell a trailing faction segment from a role-only segment (a minister whose
# meta carries a title but no party).
_FACTIONS = {"grüne", "gruene", "cdu", "spd", "fdp/dvp", "fdp", "afd", "fraktionslos"}

_HONORIFICS = ("Dr. ", "Prof. ", "Prof. Dr. ", "Dr. Dr. ", "Dr. h. c. ")


def _strip_honorifics(name: str) -> str:
    s = name.strip()
    changed = True
    while changed:
        changed = False
        for h in _HONORIFICS:
            if s.startswith(h):
                s = s[len(h):]
                changed = True
                break
    return s


def _reorder_name(name_raw: str) -> tuple[str, str, str]:
    """``"Aras Muhterem"`` (Lastname Firstname) → ``("Muhterem Aras", "Muhterem", "Aras")``.

    Returns ``(label, firstname, lastname)``. The source lists the surname
    first; we assume the first token is the surname and the remainder the given
    name(s).
    """
    name = _strip_honorifics(name_raw)
    parts = name.split()
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", parts[0]
    lastname = parts[0]
    firstname = " ".join(parts[1:])
    return f"{firstname} {lastname}", firstname, lastname


def _split_meta(meta_raw: str) -> tuple[str, str]:
    """``"| Präsidentin | GRÜNE"`` → ``("Präsidentin", "GRÜNE")``.

    Returns ``(role, faction)``; either may be empty. A trailing segment that
    matches a known faction shortcode is the faction; anything before it is the
    role. A single non-faction segment is treated as a role (e.g. a minister
    with no party).
    """
    segments = [s.strip() for s in (meta_raw or "").split("|") if s.strip()]
    if not segments:
        return "", ""
    if segments[-1].lower() in _FACTIONS:
        faction = segments[-1]
        role = " ".join(segments[:-1]).strip()
        return role, faction
    return " ".join(segments).strip(), ""


def _offset_clock(seconds: int) -> str:
    return str(timedelta(seconds=int(seconds)))  # H:MM:SS (no leading zero pad)


def _iter_parts(manifest: dict) -> list[dict]:
    """Normalise to a list of parts. Back-compat: a flat ``tops`` manifest
    (no ``parts``) is treated as a single part 1."""
    if "parts" in manifest:
        return manifest["parts"]
    return [{
        "part": manifest.get("part", 1),
        "mp4_url": manifest.get("mp4_url", ""),
        "video_page_url": manifest.get("video_page_url", ""),
        "tops": manifest.get("tops", []),
    }]


def parse_session(tops_path: Path) -> dict | None:
    with tops_path.open() as f:
        manifest = json.load(f)

    session_id = manifest["session_id"]
    iso_date = manifest["date"]
    wp = int(manifest["wp"])
    sitzungsnr = int(manifest["sitzung"])

    speeches: list[dict] = []
    for part in _iter_parts(manifest):
        part_no = int(part.get("part", 1))
        mp4_url = part.get("mp4_url", "")
        page_url = part.get("video_page_url", "")
        part_speeches: list[dict] = []
        for top in part.get("tops", []):
            for sp in top.get("speeches", []):
                label, firstname, lastname = _reorder_name(sp.get("name_raw", ""))
                role, faction = _split_meta(sp.get("meta_raw", ""))
                part_speeches.append({
                    "date": iso_date,
                    "wp": wp,
                    "sitzung_no": sitzungsnr,
                    "part": part_no,
                    "top_index": top["index"],
                    "top_title": top.get("title", "").strip(),
                    "top_description": top.get("description", "").strip(),
                    "name_raw": sp.get("name_raw", ""),
                    "label": label,
                    "firstname": firstname,
                    "lastname": lastname,
                    "role": role,
                    "gruppe": faction,
                    "start_offset": int(sp["start_offset"]),
                    "start_clock": sp.get("clock", ""),
                    "mp4_url": mp4_url,
                    "video_page_url": page_url,
                })
        # Per-speech end offset = next speech's start within THIS part (offsets
        # are relative to the part's own MP4 and reset per part).
        part_speeches.sort(key=lambda s: (s["start_offset"], s["top_index"]))
        for i, s in enumerate(part_speeches):
            s["end_offset"] = (part_speeches[i + 1]["start_offset"]
                               if i + 1 < len(part_speeches) else None)
        speeches.extend(part_speeches)

    if not speeches:
        logger.info(f"{session_id} ({iso_date}): no speeches with video — skipping")
        return None

    # Global order across parts, then a per-part cumulative synthetic base so
    # start_datetime is monotonic (offsets reset per part).
    speeches.sort(key=lambda s: (s["part"], s["start_offset"]))
    part_span = {}
    for s in speeches:
        part_span[s["part"]] = max(part_span.get(s["part"], 0), s["start_offset"])
    base_at = {}
    running = 0
    for p in sorted(part_span):
        base_at[p] = running
        running += part_span[p] + 1   # +1s nominal gap between parts

    day = datetime.strptime(iso_date, "%Y-%m-%d")
    for idx, s in enumerate(speeches, start=1):
        s["speech_index"] = idx
        s["speech_id"] = f"{session_id}_{s['part']}_{s['start_offset']}"
        g_start = base_at[s["part"]] + s["start_offset"]
        g_end = (base_at[s["part"]] + s["end_offset"]
                 if s["end_offset"] is not None else g_start)
        s["start_datetime"] = (day + timedelta(seconds=g_start)).isoformat("T", "seconds")
        s["end_datetime"] = (day + timedelta(seconds=g_end)).isoformat("T", "seconds")

    return {
        "meta": {
            "session": session_id,
            "wp": wp,
            "date": iso_date,
            "sitzung": sitzungsnr,
            "processing": {
                "parse_media": datetime.now().isoformat("T", "seconds"),
            },
        },
        "data": speeches,
    }


def parse_media_directory(media_dir: Path) -> None:
    media_dir = Path(media_dir)
    tops_files = sorted(media_dir.glob("*-tops.json"))
    if not tops_files:
        logger.warning(f"No *-tops.json manifests under {media_dir} — nothing to parse.")
        return
    for tops_path in tops_files:
        doc = parse_session(tops_path)
        if doc is None:
            continue
        session_id = doc["meta"]["session"]
        out_path = media_dir / f"{session_id}-media.json"
        out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"Wrote {out_path.name} ({len(doc['data'])} speeches, {doc['meta']['date']})")


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
