#! /usr/bin/env python3
"""Parse the downloaded DE-NW session manifests into per-Sitzung intermediate JSON.

Input per session (written by the scraper): ``original/media/{sid}-items.json``
— the ordered speeches of one Plenarsitzung, each with the raw ``TEST-REDNER``
fields (``mdlId``, ``funktionId``, name, ``fraktion``, ``funktion``, ``topNr``),
its TOP title, its seek id (``top_redner_id``) and a precise ``start_offset``
(seconds into the one session HLS stream).

This pass, per speech:

- strips academic honorifics and splits the (natural-order) ``Firstname
  Lastname`` name into ``firstname``/``lastname``/``label``;
- keeps ``fraktion`` as the ``faction`` and ``funktion`` as the chair/government
  ``role`` (the two are separate source fields — cleaner than DE-HH's overloaded
  function field);
- carries the parliament-native ``mdlId`` (MdL id; ``funktionId`` for
  chair/government speakers) as ``origin_person_id`` — the DE-NI ``abg_id`` /
  NO ``personID`` class;
- synthesises each speech's ``end_offset`` from the **next** speech's start
  (the source's rendered ``end`` is unreliable for speeches that double as a
  TOP "full length" link — the DE-BW approach); the last speech keeps its
  rendered end;
- derives **real wall-clock** ``start_datetime``/``end_datetime`` from the
  session start (``<time datetime="…+02:00">``) + the offset, when the session
  start is known (``times_are_video_relative = False``); otherwise leaves them
  ``None`` for the merger to fill video-relative;
- assigns a deterministic global ``speech_index`` in chronological order.

One record per speech is written to ``original/media/{sid}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-NW.parsers"

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-NW", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-NW", stream="media")["creator"]

# Academic titles, removed token-wise wherever they occur in the name field.
_HONORIFIC_TOKENS = {
    "dr.", "dr", "prof.", "prof", "dr.h.c.", "h.c.", "h.", "c.",
    "med.", "phil.", "jur.", "rer.", "nat.", "habil.", "dipl.",
}


def _strip_honorifics(name: str) -> str:
    toks = [t for t in name.split() if t.lower().strip(",") not in _HONORIFIC_TOKENS]
    return " ".join(toks).strip()


def _split_name(raw_name: str) -> tuple[str, str, str]:
    """``"André Kuper"`` (natural order) → ``("André Kuper", "André", "Kuper")``."""
    name = _strip_honorifics(re.sub(r'\s+', ' ', raw_name or '').strip())
    parts = name.split()
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", parts[0]
    lastname = parts[-1]
    firstname = " ".join(parts[:-1])
    return f"{firstname} {lastname}", firstname, lastname


def _iso_offset(session_start_iso: str | None, offset: int | None) -> str | None:
    """``session start + offset seconds`` as an ISO datetime, preserving the
    source timezone. ``None`` when the session start or offset is unknown."""
    if not session_start_iso or offset is None:
        return None
    try:
        base = datetime.fromisoformat(session_start_iso)
    except ValueError:
        return None
    return (base + timedelta(seconds=int(offset))).isoformat()


def parse_session(items_path: Path) -> dict | None:
    with items_path.open() as f:
        manifest = json.load(f)

    session_id = manifest["session_id"]
    wp = int(manifest["wp"])
    sitzung_no = int(manifest["sitzung"])
    iso_date = manifest.get("date") or ""
    session_start_iso = manifest.get("session_start_iso") or ""
    kid = manifest.get("kid", "")
    page_url = manifest.get("video_page_url", "")
    times_are_video_relative = not bool(session_start_iso)

    raw = manifest.get("speeches", [])
    # Chronological order: by precise start offset (falling back to source order).
    raw = sorted(
        raw,
        key=lambda s: (s.get("start_offset") if s.get("start_offset") is not None
                       else s.get("index", 0)))

    speeches: list[dict] = []
    for i, sp in enumerate(raw):
        start = sp.get("start_offset")
        # End = next speech's start; last speech keeps its (rendered) end.
        if i + 1 < len(raw):
            nxt = raw[i + 1].get("start_offset")
            end = nxt if (nxt is not None and start is not None and nxt > start) else None
        else:
            rend = sp.get("rendered_end")
            end = rend if (rend is not None and start is not None and rend > start) else None

        label, firstname, lastname = _split_name(sp.get("name", ""))
        faction = sp.get("fraktion") or ""
        role = sp.get("funktion") or ""
        speeches.append({
            "date": iso_date,
            "wp": wp,
            "sitzung_no": sitzung_no,
            "top_redner_id": sp.get("top_redner_id"),
            "origin_person_id": sp.get("mdl_id") or sp.get("funktion_id") or "",
            "mdl_id": sp.get("mdl_id") or "",
            "funktion_id": sp.get("funktion_id") or "",
            "top_nr": sp.get("top_nr") or "",
            "top_title": (sp.get("top_title") or "").strip(),
            "name_raw": sp.get("name", ""),
            "fraktion_raw": sp.get("fraktion", ""),
            "funktion_raw": sp.get("funktion", ""),
            "label": label,
            "firstname": firstname,
            "lastname": lastname,
            "role": role,
            "faction": faction,
            "start_offset": start,
            "end_offset": end,
            "kid": kid,
            "video_page_url": page_url,
            "session_start_iso": session_start_iso,
            "start_datetime": _iso_offset(session_start_iso, start),
            "end_datetime": _iso_offset(session_start_iso, end),
        })

    if not speeches:
        logger.info(f"{session_id} ({iso_date}): no speeches — skipping")
        return None

    for idx, s in enumerate(speeches, start=1):
        s["speech_index"] = idx
        s["speech_id"] = (f"{session_id}-{s['top_redner_id']}"
                          if s.get("top_redner_id") else f"{session_id}_{idx}")

    return {
        "meta": {
            "session": session_id,
            "wp": wp,
            "date": iso_date,
            "sitzung": sitzung_no,
            "kid": kid,
            "session_start_iso": session_start_iso,
            "timesAreVideoRelative": times_are_video_relative,
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
