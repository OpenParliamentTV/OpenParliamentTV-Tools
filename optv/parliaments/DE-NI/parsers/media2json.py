#! /usr/bin/env python3
"""Parse the downloaded DE-NI session manifests into per-Sitzung intermediate JSON.

Input per Sitzung (written by the scraper): ``original/media/{sid}-items.json``
— the subjects (agenda items / TOPs) of one Sitzung, each with its title, the
structured ``subjectArt`` / ``consultationType`` agenda metadata, the HLS
``streamFileName`` (+ segment length) and wall-clock stream ``startTime``, the
item's stream range and subtitle source, and, per speech, the ``speakerTiming``
spine (``abg_id``, ``surname``, ``name``, ``fraktion``, ``function``,
``speechType``, ``startTimeInStreamSecs`` / ``stopTimeInStreamSecs``).

This pass flattens subjects × speeches into one record per speech and:

- joins ``name`` (given) + ``surname`` (family) into a natural ``Firstname
  Lastname`` label (honorifics stripped) → ``firstname`` / ``lastname``;
- keeps ``fraktion`` as the ``faction`` label (the speaker's party: the chair
  keeps theirs, government members carry theirs or none);
- keeps ``function`` (e.g. ``Landtagspräsidentin``, ``Minister für …``) as the
  speaker ``role``;
- derives a speaker ``context`` from ``function`` for the chair
  (Landtagspräsident(in) → president, (Landtags)vizepräsident(in) →
  vice-president, Alterspräsident(in) → interim-president), else from ``speechType``
  (``Mitteilungen`` → president, ``KI`` / ``Zwischenfrage`` / ``pers.
  Bemerkung`` → speaker, else main-speaker);
- keeps the stream-second offsets (``start_secs`` / ``stop_secs``) for the
  merger's per-speech HLS clip URL, plus ``clip_start_secs``, the stream second
  at which that clip actually begins;
- derives **real wall-clock** ``start_datetime`` / ``end_datetime`` (UTC, ``Z``)
  from the stream ``startTime`` + the offsets. WP 18 streams have no
  ``startTime``; their start is estimated from the agenda items' local
  ``itemBeginning`` (to the minute) and flagged ``stream_start_estimated``;
- assigns a deterministic global ``speech_index`` in wall-clock order (a
  Sitzung can have several streams, each counting seconds from its own start).

One record per speech is written to ``original/media/{sid}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from optv.parliaments import get_rights as _get_rights

from ..scraper.common import clip_start_secs

logger = logging.getLogger(__name__)

BERLIN = ZoneInfo("Europe/Berlin")

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


def _context(speech_type: str, function: str = "") -> str:
    # The function is the role of this turn: a vice-president who speaks as a
    # member carries no function on that speech. "Ministerpräsident" is not
    # the chair.
    f = (function or "").strip().lower()
    if "vizepräsident" in f:            # Vizepräsident(in), Landtagsvizepräsidentin
        return "vice-president"
    if "alterspräsident" in f:
        return "interim-president"
    if f.startswith("landtagspräsident") or f.startswith("präsident"):
        return "president"
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


def _estimate_stream_starts(subjects: list[dict], iso_date: str) -> dict[str, datetime]:
    """Stream start (UTC) per stream that has no ``startTime`` (WP 18).

    ``itemBeginning`` is the local (Europe/Berlin) start of the agenda item to the
    minute, and ``item_start_secs`` its stream second, so ``itemBeginning −
    item_start_secs`` is the stream start. Only the first subject of an item
    counts (the later subjects of a multi-topic item repeat the item's
    ``itemBeginning``); the median over the stream's items absorbs the rounding.
    """
    if not iso_date:
        return {}
    cands: dict[str, list[float]] = {}
    for subj in subjects:
        name = subj.get("stream_file_name") or ""
        begin = subj.get("item_beginning") or ""
        start = subj.get("item_start_secs")
        if not name or subj.get("video_start_time") or not begin or start is None:
            continue
        if str(subj.get("subject_number")) not in ("1", "1.0"):
            continue
        try:
            local = datetime.fromisoformat(f"{iso_date}T{begin}").replace(tzinfo=BERLIN)
        except ValueError:
            continue
        cands.setdefault(name, []).append(local.timestamp() - float(start))
    return {name: datetime.fromtimestamp(statistics.median(ts), tz=timezone.utc)
            for name, ts in cands.items()}


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

    subjects = manifest.get("subjects", [])
    if subjects and any("item_start_secs" not in subj for subj in subjects):
        logger.warning(f"{session_id}: {items_path.name} lacks the agenda items' stream "
                       "range and subtitle source, so no subtitle text can be placed; "
                       "re-fetch it with --download-original --force")
    titles = {subj.get("subject_id"): subj for subj in subjects}
    estimated_starts = _estimate_stream_starts(subjects, iso_date)

    speeches: list[dict] = []
    for subject in subjects:
        stream_name = subject.get("stream_file_name", "")
        stream_start = _parse_stream_start(subject.get("video_start_time", ""))
        estimated = False
        if stream_start is None and stream_name in estimated_starts:
            stream_start = estimated_starts[stream_name]
            estimated = True
        segment = subject.get("segment_secs")
        joint = subject.get("joint_subjects") or []
        for sp in subject.get("speeches", []):
            label, first, last = _build_label(sp.get("name", ""), sp.get("surname", ""))
            faction = (sp.get("fraktion") or "").strip()
            function = (sp.get("function") or "").strip()
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
                "joint_subjects": [
                    {**j, "title": (j.get("title") or (titles.get(j.get("subject_id")) or {}).get("title") or "").strip()}
                    for j in joint
                ],
                # media / video
                "stream_file_name": stream_name,
                "video_start_time": subject.get("video_start_time", ""),
                "stream_start_estimated": estimated,
                "segment_secs": segment,
                "start_secs": start,
                "stop_secs": stop,
                "clip_start_secs": clip_start_secs(start, segment),
                # subtitles of the agenda item (or of its joint debate)
                "item_start_secs": subject.get("item_start_secs"),
                "item_stop_secs": subject.get("item_stop_secs"),
                "vtt_subject_id": subject.get("vtt_subject_id", ""),
                "vtt_offset": subject.get("vtt_source_offset", 0),
                "vtt_item_start_secs": subject.get("vtt_item_start_secs",
                                                   subject.get("item_start_secs")),
                # speaker
                "timing_id": sp.get("timing_id", ""),
                "abg_id": sp.get("abg_id"),
                "name_raw": sp.get("name", ""),
                "surname_raw": sp.get("surname", ""),
                "speech_type": sp.get("speech_type", ""),
                "label": label,
                "firstname": first,
                "lastname": last,
                "role": function,
                "faction": faction,
                "context": _context(sp.get("speech_type", ""), function),
                "start_datetime": _iso_utc(stream_start, start),
                "end_datetime": _iso_utc(stream_start, stop),
            })

    if not speeches:
        logger.info(f"{session_id} ({iso_date}): no speeches with video — skipping")
        return None

    # Global order: wall-clock start across the whole Sitzung (stream seconds
    # restart with every stream, so they only order speeches within one stream).
    speeches.sort(key=lambda s: (s["start_datetime"] or "",
                                 s["start_secs"] if s["start_secs"] is not None else 0,
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
