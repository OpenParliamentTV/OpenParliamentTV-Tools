#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-NI Sitzung from the intermediate media file.

The Plenar-TV REST API delivers the agenda (subjects) and the per-speech spine
(speaker timings) together, so the "merge" here is a single-source translation
pass (no Needleman-Wunsch). Verbatim text comes from the broadcaster's
time-aligned WebVTT subtitles: ``parsers/vtt2json.py`` parses and calibrates each
subject VTT onto the spine, and ``attach_text_by_index`` attaches the cue-timed
sentences onto each speech here (speeches with no VTT match keep
``textContents: []``). Because the cues are already time-aligned, no aeneas step
is needed. This VTT text path is **experimental and unvalidated** — see manifest.

Per-speech video is a **server-side-clipped HLS playlist** addressed directly:
``{VOD}/stream/{streamFileName}/index.m3u8?start={sec}&end={sec}`` (the clip URL
*is* the speech, so no ``#t=start,end`` media fragment is needed, unlike DE-HH).
``sourcePage`` is made unique per speech (``…#rede-{timingId}``) so the platform's
sourcePage-keyed speech identity does not collapse. Per-speech
``dateStart``/``dateEnd`` are **real wall-clock** UTC (stream ``startTime`` +
offset), so ``debug.timesAreVideoRelative`` is ``false``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-NI.merger"

from optv.shared.agenda_types import classify_de_ni

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from ..scraper.common import video_clip_uri, vtt_uri
from optv.parliaments import get_rights as _get_rights
from optv.shared.pdf2tei.spine_join import load_turns, attach_text_by_index
from optv.shared.meta import build_meta, now_iso

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-NI"
SOURCE_URI = _get_rights("DE-NI", stream="media")["sourceURI"]
# Broadcaster WebVTT subtitles, time-aligned to the clips by Plenar-TV.
PROCEEDINGS_CREATOR = _get_rights("DE-NI", stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights("DE-NI", stream="proceedings")["license"]

_SLUG_RE = re.compile(r'[^a-z0-9]+')


def _int_or_none(value):
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _agenda_id(item_number, subject_number, title: str) -> str:
    """Stable per-subject agenda id. Combined debates share an item number but
    are distinct subjects, so the subject number disambiguates."""
    item = _int_or_none(item_number)
    sub = _int_or_none(subject_number)
    if item is not None:
        return f"TOP-{item}-{sub}" if sub is not None else f"TOP-{item}"
    slug = _SLUG_RE.sub("-", (title or "").lower()).strip("-")
    if len(slug) > 64:
        slug = slug[:64].rsplit("-", 1)[0]
    return slug or "top"


def _build_person(speech: dict) -> dict:
    label = speech.get("label") or speech.get("name_raw") or "Unbekannt"
    person: dict = {
        "label": label,
        "context": speech.get("context") or "main-speaker",
    }
    if speech.get("firstname"):
        person["firstname"] = speech["firstname"]
    if speech.get("lastname"):
        person["lastname"] = speech["lastname"]
    if speech.get("role"):
        person["role"] = speech["role"]
    faction = (speech.get("faction") or "").strip()
    if faction:
        person["faction"] = {"label": faction}
    abg_id = speech.get("abg_id")
    if abg_id is not None:
        person["originPersonID"] = str(abg_id)
    return person


def _build_agenda(speech: dict) -> dict:
    title = (speech.get("top_title") or "").strip() or "(ohne Titel)"
    item = _int_or_none(speech.get("item_number"))
    official = f"TOP {item}: {title}" if item is not None else title
    out: dict = {
        "officialTitle": official,
        "title": title,
        "id": _agenda_id(speech.get("item_number"), speech.get("subject_number"), title),
    }
    native, core = classify_de_ni(
        title, speech.get("subject_art"), speech.get("consultation_type"))
    if native:
        out["nativeType"] = native
    out["type"] = core
    return out


def _build_media(speech: dict) -> dict:
    stream = speech.get("stream_file_name") or ""
    start = speech.get("start_secs")
    stop = speech.get("stop_secs")
    page = speech.get("session_page_url") or SOURCE_URI
    timing_id = speech.get("timing_id") or ""
    media: dict = {
        "videoFileURI": video_clip_uri(stream, start, stop),
        # sourcePage must be unique per speech (the platform keys speech
        # identity on it); anchor each speech by its speakerTiming UUID.
        "sourcePage": f"{page}#rede-{timing_id}" if timing_id else page,
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": speech.get("speech_id") or timing_id,
    }
    extras: dict = {}
    if start is not None:
        extras["startInStreamSecs"] = float(start)
    if stop is not None:
        extras["stopInStreamSecs"] = float(stop)
    extras["streamFileName"] = stream
    subject_id = speech.get("subject_id") or ""
    if subject_id:
        # Time-aligned WebVTT subtitles for this subject (parsed by vtt2json and
        # attached as textContents; the URI is also exposed for downstream use).
        extras["subtitleVttURI"] = vtt_uri(subject_id)
    media["additionalInformation"] = extras
    return media


def merge_session(session: str, config, options=None) -> Path:
    media_path = config.file(session, "media")
    if not media_path.exists():
        logger.warning(f"No media file for {session} at {media_path}")
        return config.file(session, "merged", create=True)

    with media_path.open() as f:
        media_doc = json.load(f)

    speeches = media_doc.get("data", [])
    if not speeches:
        logger.warning(f"{session}: media file has no speeches")
        return config.file(session, "merged", create=True)

    session_date = media_doc["meta"]["date"]
    period = int(media_doc["meta"]["period"])
    sitzung_no = int(media_doc["meta"]["sitzung"])
    tagungsabschnitt = media_doc["meta"].get("tagungsabschnitt")

    starts = [s.get("start_datetime") for s in speeches if s.get("start_datetime")]
    ends = [s.get("end_datetime") for s in speeches if s.get("end_datetime")]
    earliest = min(starts) if starts else f"{session_date}T00:00:00Z"
    latest = max(ends) if ends else f"{session_date}T23:59:59Z"

    merged: list[dict] = []
    for sp in speeches:
        date_start = sp.get("start_datetime") or earliest
        date_end = sp.get("end_datetime") or date_start
        record: dict = {
            "parliament": PARLIAMENT_ID,
            "electoralPeriod": {"number": period},
            "session": {
                "number": sitzung_no,
                "dateStart": earliest,
                "dateEnd": latest,
            },
            "dateStart": date_start,
            "dateEnd": date_end,
            "speechIndex": sp.get("speech_index") or 0,
            "originID": sp.get("speech_id") or sp.get("timing_id"),
            "originalLanguage": "de",
            "agendaItem": _build_agenda(sp),
            "people": [_build_person(sp)],
            "media": _build_media(sp),
            "textContents": [],
            "documents": [],
            "debug": {
                "source": "plenartv-api",
                "tagungsabschnitt": tagungsabschnitt,
                "subjectId": sp.get("subject_id"),
                "subjectNumber": sp.get("subject_number"),
                "itemNumber": sp.get("item_number"),
                "abgId": sp.get("abg_id"),
                "speechType": sp.get("speech_type"),
                "rednerRaw": f"{sp.get('name_raw', '')} {sp.get('surname_raw', '')}".strip(),
                "fraktionRaw": sp.get("faction"),
                "streamFileName": sp.get("stream_file_name"),
                "startInStreamSecs": sp.get("start_secs"),
                "stopInStreamSecs": sp.get("stop_secs"),
                # Per-speech dateStart/dateEnd are real wall-clock (stream
                # startTime + offset, treated as UTC), not video-relative.
                "timesAreVideoRelative": False,
            },
        }
        merged.append(record)

    # Attach Plenar-TV WebVTT text (if parsed) onto the fixed spine by speechIndex.
    turns = load_turns(config, session)
    if turns:
        matched = attach_text_by_index(merged, turns, creator=PROCEEDINGS_CREATOR,
                                       license=PROCEEDINGS_LICENSE)
        logger.info(f"{session}: attached VTT text to {matched}/{len(merged)} speeches")

    doc = {
        "meta": build_meta(
            PARLIAMENT_ID,
            session=session,
            electoral_period=period,
            date_start=earliest,
            date_end=latest,
            processing={
                **media_doc["meta"].get("processing", {}),
                "merge": now_iso(),
            },
            extra={
                "tagungsabschnitt": tagungsabschnitt,
                "sourceURI": media_doc["meta"].get("session_page_url") or SOURCE_URI,
            },
        ),
        "data": merged,
    }
    return config.save_data(doc, session, "merged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session", help="Session ID e.g. 19080")
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    from ..common import Config
    config = Config(args.data_dir)
    merge_session(args.session, config, args)
