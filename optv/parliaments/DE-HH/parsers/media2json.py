#! /usr/bin/env python3
"""Parse the downloaded DE-HH session manifests into per-Sitzung intermediate JSON.

Input per session (written by the scraper): ``original/media/{sid}-items.json``
— the agenda items (TOPs) of one Sitzung, each with its title, TOP number,
server-side HLS clip URL (clean + sign-language) and, per speech, the raw
speaker name, the ``data-speakerFunction`` string (faction or government role),
the clip-relative start offset + duration, and the per-speech ``video-download``
wall-clock unix timestamps.

This pass, per speech:

- splits the speaker name into an optional chair role prefix
  (``Präsident(in)`` / ``Vizepräsident(in)`` / …) + a clean ``Firstname
  Lastname`` (already in natural order, unlike DE-BW) → ``firstname``/``lastname``;
- maps ``data-speakerFunction`` to a ``faction`` when it is a known party, else
  treats it as a government ``role`` (``Senator(in)`` / ``Bürgermeister(in)`` / …);
- keeps the clip-relative ``start_offset`` + ``end_offset`` (= start + duration)
  for the merger's ``#t=start,end`` media fragment;
- derives **real wall-clock** ``start_datetime`` / ``end_datetime`` (UTC, ``Z``)
  from the ``video-download`` timestamps — DE-HH is the first video-only German
  Landtag with absolute per-speech times (DE-SH/DE-BY/DE-BW are video-relative);
- assigns a deterministic global ``speech_index`` ordered by (agenda order,
  start offset).

One record per speech is written to ``original/media/{sid}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-HH.parsers"

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-HH", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-HH", stream="media")["creator"]

# ``data-speakerFunction`` values that are factions (parties / faction status),
# matched case-insensitively. Anything else (e.g. "Senator") is a government
# role, not a faction.
_FACTIONS = {"spd", "grüne", "gruene", "cdu", "die linke", "linke", "afd", "fraktionslos"}

# Academic titles can appear leading ("Dr. Anke Frieling") or, in the im-en.com
# convention, after the given name ("Carola Dr. Ensslen") — so they are removed
# token-wise wherever they occur, not just at the start.
_HONORIFIC_TOKENS = {
    "dr.", "dr", "prof.", "prof", "dr.h.c.", "h.c.", "h.", "c.",
    "med.", "phil.", "jur.", "rer.", "nat.", "habil.", "dipl.",
}

# Chair role prefix at the start of the name field, e.g.
# "Präsidentin Carola Veit", "Erster Vizepräsident Mustermann".
_ROLE_PREFIX_RE = re.compile(
    r'^(?P<role>(?:(?:Erste[r]?|Zweite[r]?|Dritte[r]?|Alters)\s+)?'
    r'(?:Vize)?[Pp]räsident(?:in)?)\s+(?P<rest>.+)$')


def _strip_honorifics(name: str) -> str:
    toks = [t for t in name.split() if t.lower().strip(",") not in _HONORIFIC_TOKENS]
    return " ".join(toks).strip()


def _split_role_prefix(name_raw: str) -> tuple[str, str]:
    """``"Präsidentin Carola Veit"`` → ``("Präsidentin", "Carola Veit")``.

    Returns ``(role, clean_name)``; ``role`` is empty when the name carries no
    chair prefix.
    """
    m = _ROLE_PREFIX_RE.match(name_raw.strip())
    if m:
        return re.sub(r'\s+', ' ', m.group("role")).strip(), m.group("rest").strip()
    return "", name_raw.strip()


def _split_name(clean_name: str) -> tuple[str, str, str]:
    """``"Carola Veit"`` (natural order) → ``("Carola Veit", "Carola", "Veit")``."""
    name = _strip_honorifics(clean_name)
    parts = name.split()
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", parts[0]
    lastname = parts[-1]
    firstname = " ".join(parts[:-1])
    return f"{firstname} {lastname}", firstname, lastname


def _classify_function(function_raw: str) -> tuple[str, str]:
    """Split ``data-speakerFunction`` into ``(faction, role)``.

    A known party shortcode → faction (role empty); anything else (a government
    title like "Senator") → role (faction empty).
    """
    f = (function_raw or "").strip()
    if f.lower() in _FACTIONS:
        return f, ""
    return "", f


def _iso_utc(unix_ts: int | None) -> str | None:
    if unix_ts is None:
        return None
    return (datetime.fromtimestamp(int(unix_ts), tz=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ"))


def parse_session(items_path: Path) -> dict | None:
    with items_path.open() as f:
        manifest = json.load(f)

    session_id = manifest["session_id"]
    iso_date = manifest.get("date") or ""
    wp = int(manifest["wp"])
    sitzung_no = int(manifest["sitzung"])
    session_uuid = manifest.get("session_uuid", "")
    page_url = manifest.get("video_page_url", "")

    speeches: list[dict] = []
    for item in manifest.get("items", []):
        top_index = item.get("index")
        top_number = item.get("top_number")
        top_title = (item.get("title") or "").strip()
        clean_hls = item.get("clean_hls") or ""
        sign_hls = item.get("sign_hls") or ""
        for sp in item.get("speeches", []):
            role_prefix, clean_name = _split_role_prefix(sp.get("name_raw", ""))
            label, firstname, lastname = _split_name(clean_name)
            faction, gov_role = _classify_function(sp.get("function", ""))
            role = role_prefix or gov_role
            start = float(sp.get("start_offset") or 0)
            duration = float(sp.get("duration") or 0)
            speeches.append({
                "date": iso_date,
                "wp": wp,
                "sitzung_no": sitzung_no,
                "speech_pk": sp.get("speech_pk", ""),
                "top_index": top_index,
                "top_number": top_number,
                "top_title": top_title,
                "name_raw": sp.get("name_raw", ""),
                "function_raw": sp.get("function", ""),
                "label": label,
                "firstname": firstname,
                "lastname": lastname,
                "role": role,
                "faction": faction,
                "start_offset": start,
                "end_offset": start + duration,
                "duration": duration,
                "clean_hls": clean_hls,
                "sign_hls": sign_hls,
                "video_page_url": page_url,
                "session_uuid": session_uuid,
                "download_start": sp.get("download_start"),
                "download_stop": sp.get("download_stop"),
                "start_datetime": _iso_utc(sp.get("download_start")),
                "end_datetime": _iso_utc(sp.get("download_stop")),
            })

    if not speeches:
        logger.info(f"{session_id} ({iso_date}): no speeches with video — skipping")
        return None

    # Global order: agenda order, then clip-relative start within the TOP.
    speeches.sort(key=lambda s: (s["top_index"] if s["top_index"] is not None else 0,
                                 s["start_offset"], s["speech_pk"]))
    fallback = f"{iso_date}T00:00:00Z" if iso_date else None
    for idx, s in enumerate(speeches, start=1):
        s["speech_index"] = idx
        s["speech_id"] = s["speech_pk"] or f"{session_id}_{idx}"
        if not s["start_datetime"]:
            s["start_datetime"] = fallback
        if not s["end_datetime"]:
            s["end_datetime"] = s["start_datetime"]

    return {
        "meta": {
            "session": session_id,
            "wp": wp,
            "date": iso_date,
            "sitzung": sitzung_no,
            "session_uuid": session_uuid,
            "video_page_url": page_url,
            "processing": {
                "parse_media": datetime.now().isoformat("T", "seconds"),
            },
        },
        "data": speeches,
    }


def parse_media_directory(media_dir: Path) -> None:
    media_dir = Path(media_dir)
    items_files = sorted(media_dir.glob("*-items.json"))
    if not items_files:
        logger.warning(f"No *-items.json manifests under {media_dir} — nothing to parse.")
        return
    for items_path in items_files:
        doc = parse_session(items_path)
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
