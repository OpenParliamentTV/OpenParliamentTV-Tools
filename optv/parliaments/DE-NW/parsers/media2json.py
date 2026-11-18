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
- sets each speech's ``end_offset`` to the **next** speech's start, which keeps
  the chair's hand-over to the next speaker inside the clip. When the next listed
  speech starts more than ``UNLISTED_GAP`` seconds after the speech's own
  rendered end, an agenda item without a speech list lies in between (e.g. the
  Fragestunde), so the speech's rendered end is used instead. The chair's own
  entries (the opening of the sitting) always run to the next listed speech: the
  chair conducts everything up to it, e.g. a swearing-in. The last speech keeps
  its rendered end;
- derives **real wall-clock** ``start_datetime``/``end_datetime`` from the
  session start + the offset (``times_are_video_relative = False``), when the
  session start is known; otherwise leaves them ``None`` for the merger to fill
  video-relative. The page's ``<time datetime>`` carries the scheduled local
  start with the server's UTC offset on the day of the request, not the offset
  of the sitting's date, so only its local date and time are used and they are
  localised to ``Europe/Berlin`` (``berlin_session_start``);
- assigns a deterministic global ``speech_index`` in chronological order.

One record per speech is written to ``original/media/{sid}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from optv.parliaments import get_rights as _get_rights

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


BERLIN = ZoneInfo("Europe/Berlin")

# Seconds between a speech's rendered end and the next listed speech's start
# above which an agenda item without a speech list (Fragestunde, items without
# debate) is assumed to lie in between.
UNLISTED_GAP = 300

# ``funktion`` of a chair entry ("Präsident", "Vizepräsidentin", …).
_CHAIR_FUNKTION_RE = re.compile(r"^(?:Alters|Vize)?präsident(?:in)?$", re.IGNORECASE)


def _berlin_start(session_start_iso: str | None) -> datetime | None:
    if not session_start_iso:
        return None
    try:
        local = datetime.fromisoformat(session_start_iso).replace(tzinfo=None)
    except ValueError:
        return None
    return local.replace(tzinfo=BERLIN)


def berlin_session_start(session_start_iso: str | None) -> str:
    """The page's session start with the UTC offset of the sitting's date.

    The source stamps the scheduled local start with the server's offset on the
    day of the request; the local date and time are kept and localised to
    ``Europe/Berlin``. Returns ``""`` when the input is missing or unparseable.
    """
    start = _berlin_start(session_start_iso)
    return start.isoformat() if start else ""


def _iso_offset(session_start_iso: str | None, offset: int | None) -> str | None:
    """``session start + offset seconds`` as an ISO datetime in Europe/Berlin
    time. ``None`` when the session start or offset is unknown."""
    start = _berlin_start(session_start_iso)
    if start is None or offset is None:
        return None
    instant = start.astimezone(timezone.utc) + timedelta(seconds=int(offset))
    return instant.astimezone(BERLIN).isoformat()


def _end_offset(start: int | None, rendered_end: int | None,
                next_start: int | None, is_last: bool,
                is_chair: bool = False) -> int | None:
    """Window end for one speech (see the module docstring)."""
    if start is None:
        return None
    rend = rendered_end if (rendered_end is not None and rendered_end > start) else None
    if is_last:
        return rend
    if next_start is None or next_start <= start:
        return None
    if not is_chair and rend is not None and next_start - rend > UNLISTED_GAP:
        return rend
    return next_start


def parse_session(items_path: Path) -> dict | None:
    with items_path.open() as f:
        manifest = json.load(f)

    session_id = manifest["session_id"]
    wp = int(manifest["wp"])
    sitzung_no = int(manifest["sitzung"])
    iso_date = manifest.get("date") or ""
    session_start_iso = berlin_session_start(manifest.get("session_start_iso"))
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
        is_last = i + 1 >= len(raw)
        end = _end_offset(start, sp.get("rendered_end"),
                          None if is_last else raw[i + 1].get("start_offset"),
                          is_last,
                          is_chair=bool(_CHAIR_FUNKTION_RE.match(sp.get("funktion") or "")))

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
