#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-BW Sitzung from the intermediate media file.

There is no proceedings stream to merge — Plenarprotokolle are PDF-only and no
parser exists yet (see manifest ``supported_stages``). The mediathek chapter
list already provides per-speech segmentation with speaker, role, faction,
agenda (TOP) title and a start offset into the one session MP4, so the "merge"
here is a translation pass from the intermediate shape into Stage 2 with
``textContents: []``.

DE-BW is the SE/DE-SH per-speech-offset model: one session recording, per-speech
windows addressed by an HTML5 media-fragment ``#t=start,end`` on
``videoFileURI`` plus a ``startOffset`` in ``additionalInformation``. The
``sourcePage`` is made unique per speech (``…#t=start``) so the platform's
sourcePage-keyed speech identity does not collapse distinct speeches.
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
    __package__ = "optv.parliaments.DE-BW.merger"

from optv.shared.agenda_types import classify_de_bw

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from optv.parliaments import get_rights as _get_rights
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import load_turns, join_text_to_spine

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-BW"
SOURCE_URI = _get_rights("DE-BW", stream="media")["sourceURI"]
PROCEEDINGS_CREATOR = "Landtag von Baden-Württemberg"
PROCEEDINGS_LICENSE = "Amtliches Werk (§ 5 Abs. 2 UrhG)"

# "TOP 4 Zweite Beratung" / "Fortsetzung TOP 4 …" → both yield TOP number 4, so a
# debate split across video parts collapses to one agendaItem. Letters are kept
# ("TOP 4a") to distinguish sub-items.
_TOP_NO_RE = re.compile(r'\bTOP\s+(?P<no>\d+[a-z]?)', re.I)
_SLUG_RE = re.compile(r'[^a-z0-9]+')


def _agenda_id(title: str) -> str:
    """Stable per-TOP id from the title — `TOP-{n}` when numbered, else a slug.

    Numbered ids are shared across parts (so `Fortsetzung TOP 4` merges into
    `TOP 4`); unnumbered headers ("Beginn der Sitzung") get a title slug.
    """
    m = _TOP_NO_RE.search(title or "")
    if m:
        return f"TOP-{m.group('no').lower()}"
    slug = _SLUG_RE.sub("-", (title or "").lower()).strip("-")
    return slug or "top"


def _speaker_context(role: str) -> str:
    r = (role or "").lower()
    if ("vizepräsident" in r or "vizepraesident" in r
            or ("stellv" in r and ("präsident" in r or "praesident" in r))):
        return "vice-president"
    if "ministerpräsident" in r or "ministerpraesident" in r:
        return "main-speaker"   # speaks as head of government, not as chair
    if "präsident" in r or "praesident" in r:
        return "president"
    return "main-speaker"


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
    party = (speech.get("gruppe") or "").strip()
    if party:
        person["faction"] = {"label": party}
    return person


def _build_agenda(speech: dict) -> dict:
    title = (speech.get("top_title") or "").strip() or "(ohne Titel)"
    desc = (speech.get("top_description") or "").strip()
    out: dict = {
        "officialTitle": f"{title}: {desc}" if desc else title,
        "title": title,
        "id": _agenda_id(title),
    }
    native, core = classify_de_bw(f"{title} {desc}")
    if native:
        out["nativeType"] = native
    out["type"] = core
    return out


def _build_media(speech: dict) -> dict:
    mp4 = speech.get("mp4_url") or ""
    start = speech.get("start_offset")
    end = speech.get("end_offset")
    if end is not None:
        fragment = f"#t={start},{end}"
    else:
        fragment = f"#t={start}"
    page = speech.get("video_page_url") or SOURCE_URI
    media: dict = {
        "videoFileURI": f"{mp4}{fragment}" if mp4 else "",
        # sourcePage must be unique per speech (the platform keys speech
        # identity on it); the page is one URL per session, so append the
        # per-speech offset fragment.
        "sourcePage": f"{page}#t={start}",
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": speech["speech_id"],
    }
    extras: dict = {"startOffset": float(start)}
    if end is not None:
        extras["endOffset"] = float(end)
    if speech.get("start_clock"):
        extras["clockStart"] = speech["start_clock"]
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
                "source": "mediathek-chapterlist",
                "part": sp.get("part"),
                "topIndex": sp.get("top_index"),
                "rednerRaw": sp.get("name_raw"),
                "gruppeRaw": sp.get("gruppe"),
                "roleRaw": sp.get("role"),
                "startOffset": sp.get("start_offset"),
                "endOffset": sp.get("end_offset"),
                # Times are video-relative (offset from the session-video
                # origin), not wall-clock — the source carries no per-speech
                # absolute timestamp.
                "timesAreVideoRelative": True,
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
    parser.add_argument("session", help="Session ID e.g. 17118")
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
