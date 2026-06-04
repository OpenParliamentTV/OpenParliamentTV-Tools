#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-NW Plenarsitzung from the intermediate media file.

There is no proceedings stream to merge — Plenarprotokolle are PDF-only
(``MMP18-{N}.pdf``) and no parser exists yet (see manifest ``supported_stages``).
The mediathek session page already provides per-speech segmentation with speaker,
MdL id, role, faction, per-TOP agenda title and a precise start offset, so the
"merge" here is a single-source translation pass (no Needleman-Wunsch) from the
intermediate shape into Stage 2 with ``textContents: []``.

DE-NW is the SE/DE-SH per-speech-offset model: one HLS stream per session and
per-speech windows addressed by an HTML5 media-fragment ``#t=start,end`` on
``videoFileURI`` (offsets into that one stream) plus ``startOffset``/``endOffset``
in ``additionalInformation``. ``sourcePage`` is the session page with the
per-speech ``&top-redner-id={id}`` query — already unique per speech, so the
platform's sourcePage-keyed speech identity does not collapse distinct speeches.
The speech ``dateStart``/``dateEnd`` are **real wall-clock** (session start from
the page header + offset), so ``debug.timesAreVideoRelative`` is ``false`` (the
source stamps ``+02:00`` even in winter — emitted as-is).
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
    __package__ = "optv.parliaments.DE-NW.merger"

from optv.shared.agenda_types import classify_de_nw
from optv.shared.merge_format import format_offset as _num
from optv.shared.lang.de import speaker_context as _speaker_context

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from ..scraper.common import hls_url, video_page_url
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-NW"
SOURCE_URI = _get_rights("DE-NW", stream="media")["sourceURI"]

_SLUG_RE = re.compile(r'[^a-z0-9]+')


def _agenda_id(top_number: str, title: str) -> str:
    """``TOP-{n}`` when the item is numbered, else a (length-capped) title slug."""
    if top_number:
        return f"TOP-{str(top_number).lower()}"
    slug = _SLUG_RE.sub("-", (title or "").lower()).strip("-")
    if len(slug) > 64:
        slug = slug[:64].rsplit("-", 1)[0]   # cut at a word boundary
    return slug or "eroeffnung"


def _build_person(speech: dict) -> dict:
    label = speech.get("label") or speech.get("name_raw") or "Unbekannt"
    person: dict = {
        "label": label,
        "context": _speaker_context(speech.get("role", "")),
    }
    if speech.get("firstname"):
        person["firstname"] = speech["firstname"]
    if speech.get("lastname"):
        person["lastname"] = speech["lastname"]
    if speech.get("role"):
        person["role"] = speech["role"]
    if speech.get("origin_person_id"):
        person["originPersonID"] = speech["origin_person_id"]
    faction = (speech.get("faction") or "").strip()
    if faction:
        person["faction"] = {"label": faction}
    return person


def _build_agenda(speech: dict) -> dict:
    title = (speech.get("top_title") or "").strip() or "(ohne Titel)"
    top_number = speech.get("top_nr") or ""
    official = f"TOP {top_number}: {title}" if top_number else title
    out: dict = {
        "officialTitle": official,
        "title": title,
        "id": _agenda_id(top_number, title),
    }
    native, core = classify_de_nw(title)
    if native:
        out["nativeType"] = native
    out["type"] = core
    return out


def _build_media(speech: dict) -> dict:
    kid = speech.get("kid") or ""
    stream = hls_url(kid) if kid else ""
    start = speech.get("start_offset")
    end = speech.get("end_offset")
    if start is not None and end is not None:
        fragment = f"#t={_num(start)},{_num(end)}"
    elif start is not None:
        fragment = f"#t={_num(start)}"
    else:
        fragment = ""
    rid = speech.get("top_redner_id")
    # sourcePage is unique per speech (the per-speech seek query) — the platform
    # keys speech identity on it.
    page = video_page_url(kid, rid) if (kid and rid) else (speech.get("video_page_url") or SOURCE_URI)
    media: dict = {
        "videoFileURI": f"{stream}{fragment}" if stream else "",
        "sourcePage": page,
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": rid or speech.get("speech_id") or "",
    }
    extras: dict = {}
    if start is not None:
        extras["startOffset"] = float(start)
    if end is not None:
        extras["endOffset"] = float(end)
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

    meta = media_doc["meta"]
    session_date = meta["date"]
    wp = int(meta["wp"])
    sitzung_no = int(meta["sitzung"])
    times_video_relative = bool(meta.get("timesAreVideoRelative"))

    starts = [s.get("start_datetime") for s in speeches if s.get("start_datetime")]
    ends = [s.get("end_datetime") for s in speeches if s.get("end_datetime")]
    fallback_start = meta.get("session_start_iso") or f"{session_date}T00:00:00Z"
    earliest = min(starts) if starts else fallback_start
    latest = max(ends) if ends else (max(starts) if starts else f"{session_date}T23:59:59Z")

    merged: list[dict] = []
    for sp in speeches:
        date_start = sp.get("start_datetime") or earliest
        date_end = sp.get("end_datetime") or date_start
        record: dict = {
            "parliament": PARLIAMENT_ID,
            "electoralPeriod": {"number": wp},
            "session": {
                "number": sitzung_no,
                "dateStart": earliest,
                "dateEnd": latest,
            },
            "dateStart": date_start,
            "dateEnd": date_end,
            "speechIndex": sp.get("speech_index") or 0,
            "originID": sp.get("speech_id") or sp.get("top_redner_id"),
            "originalLanguage": "de",
            "agendaItem": _build_agenda(sp),
            "people": [_build_person(sp)],
            "media": _build_media(sp),
            "textContents": [],
            "documents": [],
            "debug": {
                "source": "mediathek-video-page",
                "kid": sp.get("kid"),
                "topRednerID": sp.get("top_redner_id"),
                "topNumber": sp.get("top_nr"),
                "mdlId": sp.get("mdl_id"),
                "funktionId": sp.get("funktion_id"),
                "rednerRaw": sp.get("name_raw"),
                "fraktionRaw": sp.get("fraktion_raw"),
                "funktionRaw": sp.get("funktion_raw"),
                "startOffset": sp.get("start_offset"),
                "endOffset": sp.get("end_offset"),
                # Per-speech dateStart/dateEnd derive from the session-start
                # header + offset — real wall-clock unless the header was absent.
                "timesAreVideoRelative": times_video_relative,
            },
        }
        merged.append(record)

    doc = {
        "meta": {
            "schemaVersion": "1.0",
            "parliament": PARLIAMENT_ID,
            "electoralPeriod": {"number": wp},
            "session": session,
            "dateStart": earliest,
            "dateEnd": latest,
            "sourceURI": meta.get("video_page_url") or SOURCE_URI,
            "processing": {
                **meta.get("processing", {}),
                "merge": datetime.now().isoformat("T", "seconds"),
            },
            "lastUpdate": datetime.now().isoformat("T", "seconds"),
        },
        "data": merged,
    }
    return config.save_data(doc, session, "merged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session", help="Session ID e.g. 18117")
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
