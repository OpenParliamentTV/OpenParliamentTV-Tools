#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-SN Sitzung from the intermediate media file.

The mediathek list provides the per-speech spine (speaker, faction, agenda (TOP)
text and a start/end offset into the one daily HLS stream). The Plenarprotokoll
PDF is parsed via ``optv.shared.pdf2tei`` and joined onto that spine here
(``join_text_to_spine``), so each matched speech carries verbatim ``textContents``
(speeches with no text match keep ``textContents: []``). This text join is
**experimental and unvalidated** — see manifest.

DE-SN is the SE/DE-SH/DE-BW per-speech-offset model: one daily recording,
per-speech windows addressed by an HTML5 media-fragment ``#t=start,end`` on
``videoFileURI`` plus ``startOffset``/``endOffset`` in ``additionalInformation``.
Unlike DE-BW, both offsets are present in the source and each item carries a real
wall-clock time, so ``dateStart``/``dateEnd`` are absolute (``debug.
timesAreVideoRelative = False``). The per-speech ``sourcePage`` is the
Einzelbeitrag URL — already unique per speech, so the platform's sourcePage-keyed
speech identity does not collapse distinct speeches.
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
    __package__ = "optv.parliaments.DE-SN.merger"

from optv.shared.agenda_types import classify_de_sn

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from optv.parliaments import get_rights as _get_rights
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import load_turns, join_text_to_spine

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-SN"
SOURCE_URI = _get_rights("DE-SN", stream="media")["sourceURI"]
PROCEEDINGS_CREATOR = "Sächsischer Landtag"
PROCEEDINGS_LICENSE = "Amtliches Werk (§ 5 Abs. 2 UrhG)"

_SLUG_RE = re.compile(r'[^a-z0-9]+')


def _agenda_id(top_no: str | None, title: str) -> str:
    """Stable per-TOP id — ``TOP-{n}`` when numbered, else a title slug."""
    if top_no:
        return f"TOP-{top_no.lower()}"
    slug = _SLUG_RE.sub("-", (title or "").lower()).strip("-")
    return slug or "top"


def _build_person(speech: dict) -> dict:
    label = speech.get("label") or speech.get("name_raw") or "Unbekannt"
    person: dict = {
        "label": label,
        "context": speech.get("context", "main-speaker"),
    }
    if speech.get("firstname"):
        person["firstname"] = speech["firstname"]
    if speech.get("lastname"):
        person["lastname"] = speech["lastname"]
    if speech.get("role"):
        person["role"] = speech["role"]
    party = (speech.get("gruppe") or "").strip()
    if party:
        person["faction"] = {"label": party}
    return person


def _build_agenda(speech: dict) -> dict:
    top_no = speech.get("top_no")
    title = (speech.get("top_title") or "").strip()
    if not title:
        title = f"TOP {top_no}" if top_no else "(ohne Titel)"
    out: dict = {
        "officialTitle": title,
        "title": title,
        "id": _agenda_id(top_no, title),
    }
    native, core = classify_de_sn(f"{title} {speech.get('speech_type', '')}")
    if native:
        out["nativeType"] = native
    out["type"] = core
    return out


def _build_media(speech: dict) -> dict:
    smil = speech.get("smil_url") or ""
    start = speech.get("start_offset")
    end = speech.get("end_offset")
    if end is not None:
        fragment = f"#t={start},{end}"
    else:
        fragment = f"#t={start}"
    media: dict = {
        "videoFileURI": f"{smil}{fragment}" if smil else "",
        # sourcePage is the per-speech Einzelbeitrag URL — already unique.
        "sourcePage": speech.get("source_page") or SOURCE_URI,
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": speech["einzelbeitrag_id"],
    }
    extras: dict = {}
    if start is not None:
        extras["startOffset"] = float(start)
    if end is not None:
        extras["endOffset"] = float(end)
    if speech.get("start_clock"):
        extras["clockStart"] = speech["start_clock"]
    if extras:
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
    earliest = min(starts) if starts else f"{session_date}T00:00:00"
    latest = max(ends) if ends else f"{session_date}T23:59:59"

    merged: list[dict] = []
    for sp in speeches:
        date_start = sp.get("start_datetime") or f"{session_date}T00:00:00"
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
            "originID": sp["speech_id"],
            "originalLanguage": "de",
            "agendaItem": _build_agenda(sp),
            "people": [_build_person(sp)],
            "media": _build_media(sp),
            "textContents": [],
            "documents": [],
            "debug": {
                "source": "mediathek-list",
                "einzelbeitragID": sp.get("einzelbeitrag_id"),
                "topNo": sp.get("top_no"),
                "rednerRaw": sp.get("name_raw"),
                "gruppeRaw": sp.get("gruppe") or sp.get("role"),
                "speechType": sp.get("speech_type"),
                "startOffset": sp.get("start_offset"),
                "endOffset": sp.get("end_offset"),
                # The mediathek item carries a real per-speech wall-clock time,
                # so dateStart/dateEnd are absolute (not video-relative).
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
            "sourceURI": SOURCE_URI,
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
    parser.add_argument("session", help="Session ID e.g. 08025")
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
