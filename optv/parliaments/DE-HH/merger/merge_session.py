#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-HH Sitzung from the intermediate media file.

The mediathek session page provides the per-speech spine (speaker, role, faction,
per-TOP agenda title and a clip-relative start offset). The Plenarprotokoll PDF
(ParlDok) is parsed via ``optv.shared.pdf2tei`` and joined onto that spine here
(``join_text_to_spine``), so each matched speech carries verbatim ``textContents``
(speeches with no text match keep ``textContents: []``). This text join is
**experimental and unvalidated** — see manifest.

DE-HH is the SE/DE-SH per-speech-offset model with a per-TOP twist: each
Tagesordnungspunkt is its own server-side HLS clip, and per-speech windows are
addressed by an HTML5 media-fragment ``#t=start,end`` on ``videoFileURI`` (the
offsets are relative to that TOP's clip) plus ``startOffset``/``endOffset`` in
``additionalInformation``. ``sourcePage`` is made unique per speech
(``…#rede-{speechPk}``, the mediathek's own per-speech anchor) so the platform's
sourcePage-keyed speech identity does not collapse distinct speeches. Unlike
DE-BW, the speech ``dateStart``/``dateEnd`` are **real wall-clock** times (from
the per-speech video-download timestamps), so ``debug.timesAreVideoRelative`` is
``false``.
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
    __package__ = "optv.parliaments.DE-HH.merger"

from optv.shared.agenda_types import classify_de_hh
from optv.shared.merge_format import agenda_id as _agenda_id, format_offset as _num
from optv.shared.lang.de import speaker_context as _speaker_context

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from optv.parliaments import get_rights as _get_rights
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import load_turns, join_text_to_spine

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-HH"
SOURCE_URI = _get_rights("DE-HH", stream="media")["sourceURI"]
PROCEEDINGS_CREATOR = "Hamburgische Bürgerschaft"
PROCEEDINGS_LICENSE = "Amtliches Werk (§ 5 Abs. 2 UrhG)"

_SLUG_RE = re.compile(r'[^a-z0-9]+')


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
    faction = (speech.get("faction") or "").strip()
    if faction:
        person["faction"] = {"label": faction}
    return person


def _build_agenda(speech: dict) -> dict:
    title = (speech.get("top_title") or "").strip() or "(ohne Titel)"
    top_number = speech.get("top_number")
    official = f"TOP {top_number}: {title}" if top_number else title
    out: dict = {
        "officialTitle": official,
        "title": title,
        "id": _agenda_id(top_number, title),
    }
    native, core = classify_de_hh(title)
    if native:
        out["nativeType"] = native
    out["type"] = core
    return out


def _build_media(speech: dict) -> dict:
    clean = speech.get("clean_hls") or ""
    start = speech.get("start_offset")
    end = speech.get("end_offset")
    fragment = f"#t={_num(start)},{_num(end)}" if end is not None else f"#t={_num(start)}"
    page = speech.get("video_page_url") or SOURCE_URI
    pk = speech.get("speech_pk") or ""
    media: dict = {
        "videoFileURI": f"{clean}{fragment}" if clean else "",
        # sourcePage must be unique per speech (the platform keys speech
        # identity on it); the mediathek anchors each speech at #rede-{pk}.
        "sourcePage": f"{page}#rede-{pk}" if pk else page,
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": speech.get("speech_id") or pk,
    }
    extras: dict = {}
    if start is not None:
        extras["startOffset"] = float(start)
    if end is not None:
        extras["endOffset"] = float(end)
    sign = speech.get("sign_hls") or ""
    if sign:
        extras["signLanguageVideoFileURI"] = f"{sign}{fragment}"
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
    wp = int(media_doc["meta"]["wp"])
    sitzung_no = int(media_doc["meta"]["sitzung"])

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
            "electoralPeriod": {"number": wp},
            "session": {
                "number": sitzung_no,
                "dateStart": earliest,
                "dateEnd": latest,
            },
            "dateStart": date_start,
            "dateEnd": date_end,
            "speechIndex": sp.get("speech_index") or 0,
            "originID": sp.get("speech_id") or sp.get("speech_pk"),
            "originalLanguage": "de",
            "agendaItem": _build_agenda(sp),
            "people": [_build_person(sp)],
            "media": _build_media(sp),
            "textContents": [],
            "documents": [],
            "debug": {
                "source": "mediathek-sessionpage",
                "sessionUUID": sp.get("session_uuid"),
                "topIndex": sp.get("top_index"),
                "topNumber": sp.get("top_number"),
                "rednerRaw": sp.get("name_raw"),
                "functionRaw": sp.get("function_raw"),
                "roleRaw": sp.get("role"),
                "startOffset": sp.get("start_offset"),
                "endOffset": sp.get("end_offset"),
                "duration": sp.get("duration"),
                # Per-speech dateStart/dateEnd are real wall-clock (from the
                # mediathek video-download timestamps), not video-relative.
                "timesAreVideoRelative": False,
            },
        }
        merged.append(record)

    # Spine-join: attach proceedings text (if parsed) onto the fixed media spine.
    turns = load_turns(config, session)
    if turns:
        spine_keys = [_match_key(sp.get("label") or sp.get("name_raw") or "") for sp in speeches]
        matched = join_text_to_spine(merged, spine_keys, turns,
                                     creator=PROCEEDINGS_CREATOR,
                                     license=PROCEEDINGS_LICENSE)
        logger.info(f"{session}: matched {matched}/{len(merged)} speeches to "
                    f"{len(turns)} proceedings turns")

    doc = {
        "meta": {
            "schemaVersion": "1.0",
            "parliament": PARLIAMENT_ID,
            "electoralPeriod": {"number": wp},
            "session": session,
            "dateStart": earliest,
            "dateEnd": latest,
            "sourceURI": media_doc["meta"].get("video_page_url") or SOURCE_URI,
            "processing": {
                **media_doc["meta"].get("processing", {}),
                "merge": datetime.now().isoformat("T", "seconds"),
            },
            "lastUpdate": datetime.now().isoformat("T", "seconds"),
        },
        "data": merged,
    }
    return config.save_data(doc, session, "merged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session", help="Session ID e.g. 23018")
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
