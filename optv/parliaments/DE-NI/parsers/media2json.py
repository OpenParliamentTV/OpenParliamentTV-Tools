#! /usr/bin/env python3
"""Parse the downloaded DE-NI session manifests into per-Sitzung intermediate JSON.

Input per Sitzung (written by the scraper): ``original/media/{sid}-items.json``
— the subjects (agenda items / TOPs) of one Sitzung, each with its title, the
structured ``subjectArt`` / ``consultationType`` agenda metadata, the per-Sitzung
HLS ``streamFileName`` + wall-clock stream ``startTime``, and, per speech, the
``speakerTiming`` spine (``abg_id``, ``surname``, ``name``, ``fraktion``,
``speechType``, ``startTimeInStreamSecs`` / ``stopTimeInStreamSecs``).

This pass flattens subjects × speeches into one record per speech and:

- joins ``name`` (given) + ``surname`` (family) into a natural ``Firstname
  Lastname`` label (honorifics stripped) → ``firstname`` / ``lastname``;
- keeps ``fraktion`` as the ``faction`` label (it is **always** the speaker's
  party in DE-NI — even the presiding chair retains their party — unlike DE-HH's
  overloaded function field);
- derives a speaker ``context`` from ``speechType`` (``Mitteilungen`` → chair,
  ``KI`` / ``Zwischenfrage`` / ``pers. Bemerkung`` → secondary, else main);
- keeps the stream-second offsets (``start_secs`` / ``stop_secs``) for the
  merger's per-speech HLS clip URL;
- derives **real wall-clock** ``start_datetime`` / ``end_datetime`` (UTC, ``Z``)
  from the stream ``startTime`` + the offsets;
- assigns a deterministic global ``speech_index`` ordered by stream start.

One record per speech is written to ``original/media/{sid}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-NI.parsers"

logger = logging.getLogger(__name__)

MEDIA_CREATOR = _get_rights("DE-NI", stream="media")["creator"]
MEDIA_LICENSE = _get_rights("DE-NI", stream="media")["license"]

# Academic titles, removed token-wise wherever they occur.
_HONORIFIC_TOKENS = {
    "dr.", "dr", "prof.", "prof", "dr.h.c.", "h.c.", "h.", "c.",
    "med.", "phil.", "jur.", "rer.", "nat.", "habil.", "dipl.",
}

# speechType → speaker context. "Mitteilungen" is the presiding chair's
# announcements; KI (Kurzintervention) / Zwischenfrage / personal remarks are
# secondary interventions; everything else (RZ regular, BE Berichterstatter,
# Antwort government reply) is a main speaker turn.
_CONTEXT_BY_SPEECH_TYPE = {
    "mitteilungen": "president",
    "ki": "speaker",
    "kurzintervention": "speaker",
    "zwischenfrage": "speaker",
    "pers. bemerkung": "speaker",
    "persönliche bemerkung": "speaker",
}


def _strip_honorifics(name: str) -> str:
    toks = [t for t in name.split() if t.lower().strip(",") not in _HONORIFIC_TOKENS]
    return " ".join(toks).strip()


def _build_label(name: str, surname: str) -> tuple[str, str, str]:
    """``("Laura", "Hopmann")`` → ``("Laura Hopmann", "Laura", "Hopmann")``."""
    first = _strip_honorifics(name or "")
    last = _strip_honorifics(surname or "")
    label = " ".join(p for p in (first, last) if p)
    return label, first, last


def _context(speech_type: str) -> str:
    return _CONTEXT_BY_SPEECH_TYPE.get((speech_type or "").strip().lower(), "main-speaker")


def _parse_stream_start(value: str) -> datetime | None:
    """Parse the stream ``startTime`` (naive ISO, treated as UTC)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _iso_utc(start: datetime | None, offset_secs) -> str | None:
    if start is None or offset_secs is None:
        return None
    return ((start + timedelta(seconds=float(offset_secs)))
            .astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))


def parse_session(items_path: Path) -> dict | None:
    with items_path.open() as f:
        manifest = json.load(f)

    session_id = manifest["session_id"]
    iso_date = manifest.get("date") or ""
    period = int(manifest["period"])
    sitzung_no = int(manifest["sitzung"])
    tagungsabschnitt = manifest.get("tagungsabschnitt")
    page_url = manifest.get("session_page_url", "")

    speeches: list[dict] = []
    for subject in manifest.get("subjects", []):
        stream_start = _parse_stream_start(subject.get("video_start_time", ""))
        for sp in subject.get("speeches", []):
            label, first, last = _build_label(sp.get("name", ""), sp.get("surname", ""))
            faction = (sp.get("fraktion") or "").strip()
            start = sp.get("start_secs")
            stop = sp.get("stop_secs")
            speeches.append({
                "date": iso_date,
                "period": period,
                "sitzung_no": sitzung_no,
                "tagungsabschnitt": tagungsabschnitt,
                "session_page_url": page_url,
                # agenda (subject) fields
                "subject_id": subject.get("subject_id", ""),
                "subject_number": subject.get("subject_number"),
                "item_number": subject.get("item_number"),
                "top_title": (subject.get("title") or "").strip(),
                "subject_art": subject.get("subject_art", ""),
                "consultation_type": subject.get("consultation_type", ""),
                "applicant": subject.get("applicant", ""),
                "incoming_print": subject.get("incoming_print", ""),
                "incoming_print_link": subject.get("incoming_print_link", ""),
                # media / video
                "stream_file_name": subject.get("stream_file_name", ""),
                "video_start_time": subject.get("video_start_time", ""),
                "start_secs": start,
                "stop_secs": stop,
                # speaker
                "timing_id": sp.get("timing_id", ""),
                "abg_id": sp.get("abg_id"),
                "name_raw": sp.get("name", ""),
                "surname_raw": sp.get("surname", ""),
                "speech_type": sp.get("speech_type", ""),
                "label": label,
                "firstname": first,
                "lastname": last,
                "role": "",
                "faction": faction,
                "context": _context(sp.get("speech_type", "")),
                "start_datetime": _iso_utc(stream_start, start),
                "end_datetime": _iso_utc(stream_start, stop),
            })

    if not speeches:
        logger.info(f"{session_id} ({iso_date}): no speeches with video — skipping")
        return None

    # Global order: stream start second across the whole Sitzung.
    speeches.sort(key=lambda s: (s["start_secs"] if s["start_secs"] is not None else 0,
                                 s["timing_id"]))
    fallback = f"{iso_date}T00:00:00Z" if iso_date else None
    for idx, s in enumerate(speeches, start=1):
        s["speech_index"] = idx
        s["speech_id"] = s["timing_id"] or f"{session_id}_{idx}"
        if not s["start_datetime"]:
            s["start_datetime"] = fallback
        if not s["end_datetime"]:
            s["end_datetime"] = s["start_datetime"]

    return {
        "meta": {
            "session": session_id,
            "period": period,
            "date": iso_date,
            "sitzung": sitzung_no,
            "tagungsabschnitt": tagungsabschnitt,
            "session_page_url": page_url,
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
