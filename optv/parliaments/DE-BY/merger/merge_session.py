#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-BY Sitzung from the intermediate media file.

There is no proceedings stream to merge — Plenarprotokolle are PDF-only and no
parser exists yet (see manifest ``supported_stages``). The "Plenum Online"
per-TOP playlists already provide per-speech segmentation with speaker, party,
agenda title and a per-speech HLS master, so the "merge" here is a translation
pass from the intermediate shape into Stage 2 with ``textContents: []``.
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
    __package__ = "optv.parliaments.DE-BY.merger"

from optv.shared.agenda_types import classify_de_by
from optv.shared.merge_format import split_first_last as _split_first_last
from optv.shared.lang.de import strip_honorifics as _strip_honorifics
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import load_turns, join_text_to_spine

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-BY"
SOURCE_URI = _get_rights("DE-BY", stream="media")["sourceURI"]
# Verbatim Plenarprotokoll text is an amtliches Werk (§ 5 Abs. 2 UrhG, free to
# reuse). The video clip carries its own (restrictive) media license separately.
PROCEEDINGS_CREATOR = "Bayerischer Landtag"
PROCEEDINGS_LICENSE = "Amtliches Werk (§ 5 Abs. 2 UrhG)"

# Presiding-officer + government roles that appear as a prefix on the speaker
# title when there is no party parenthetical. Ordinals ("Erster", "Zweite", …)
# precede Vizepräsident(in) for the Bavarian Landtag's several deputies.
_ORDINAL = (r'(?:Erste[rn]?|Zweite[rn]?|Dritte[rn]?|Vierte[rn]?|F[üu]nfte[rn]?|'
            r'Sechste[rn]?|Siebte[rn]?)')
_NAME_ROLE_RE = re.compile(
    r'^\s*(?P<role>'
    r'Landtagsvizepr[äa]sident(?:in)?|Landtagspr[äa]sident(?:in)?|'
    rf'(?:{_ORDINAL}\s+)?Vizepr[äa]sident(?:in)?|'
    r'Ministerpr[äa]sident(?:in)?|Pr[äa]sident(?:in)?|'
    r'Staatsminister(?:in)?|Staatssekret[äa]r(?:in)?|Minister(?:in)?'
    r')\s+(?P<rest>.+?)\s*$',
    re.UNICODE,
)

_HONORIFICS = ("Dr. ", "Prof. ", "Prof. Dr. ", "Dr. Dr. ", "Dr. h. c. ")


def _split_role_and_name(redner: str) -> tuple[str | None, str]:
    if not redner:
        return None, redner
    m = _NAME_ROLE_RE.match(redner.strip())
    if not m:
        return None, redner.strip()
    return m.group("role"), m.group("rest").strip()


def _speaker_context(role: str | None) -> str:
    r = (role or "").lower()
    if "vizepräsident" in r or "vizepraesident" in r:
        return "vice-president"
    if "präsident" in r or "praesident" in r:
        # Ministerpräsident speaks as government, not as chair.
        if "ministerpräsident" in r or "ministerpraesident" in r:
            return "main-speaker"
        return "president"
    return "main-speaker"


def _build_person(speech: dict) -> dict:
    role, plain_name = _split_role_and_name(speech.get("redner", ""))
    plain_name = _strip_honorifics(plain_name)
    person: dict = {
        "label": plain_name or speech.get("redner", "") or "Unbekannt",
        "context": _speaker_context(role),
    }
    first, last = _split_first_last(plain_name)
    if first:
        person["firstname"] = first
    if last:
        person["lastname"] = last
    if role:
        person["role"] = role
    party = (speech.get("gruppe") or "").strip()
    if party:
        person["faction"] = {"label": party}
    return person


def _build_agenda(speech: dict) -> dict:
    title = (speech.get("top_title") or "").strip()
    out: dict = {
        "officialTitle": title or "(ohne Titel)",
        "title": title or "(ohne Titel)",
        "id": f"TOP-{speech.get('top_index')}",
    }
    native, core = classify_de_by(title)
    if native:
        out["nativeType"] = native
    out["type"] = core
    return out


def _build_media(speech: dict) -> dict:
    media: dict = {
        "videoFileURI": speech.get("videoFileURI") or "",
        "sourcePage": speech.get("sourcePage") or SOURCE_URI,
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": speech["speech_id"],
    }
    extras = {
        "metaVodId": speech.get("meta_vod_id"),
        "playlistURI": speech.get("meta_vod_url"),
        "startId": speech.get("item_id"),
        "clockStart": speech.get("start_clock"),
    }
    extras = {k: v for k, v in extras.items() if v not in (None, "")}
    if extras:
        media["additionalInformation"] = extras
    return media


def merge_session(session: str, config, options) -> Path:
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
    earliest = min(starts) if starts else f"{session_date}T00:00:00"
    latest = max(starts) if starts else f"{session_date}T23:59:59"

    merged: list[dict] = []
    for sp in speeches:
        date_start = sp.get("start_datetime") or f"{session_date}T00:00:00"
        # No per-speech end time in the source — the HLS master IS the speech
        # clip, so we record a point start and leave the end equal to it.
        date_end = date_start
        record: dict = {
            "parliament": PARLIAMENT_ID,
            "electoralPeriod": {"number": wp},
            "session": {
                "number": sitzung_no,
                "dateStart": f"{earliest}",
                "dateEnd": f"{latest}",
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
                "source": "plon",
                "topIndex": sp.get("top_index"),
                "rednerRaw": sp.get("speaker_raw"),
                "gruppeRaw": sp.get("gruppe"),
                "clockStart": sp.get("start_clock"),
                "metaVodId": sp.get("meta_vod_id"),
            },
        }
        merged.append(record)

    # Spine-join: attach proceedings text (if parsed) onto the fixed media spine.
    turns = load_turns(config, session)
    if turns:
        spine_keys = [_match_key(sp.get("redner") or sp.get("speaker_raw") or "")
                      for sp in speeches]
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
    parser.add_argument("session", help="Session ID e.g. 19054")
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
