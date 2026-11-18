#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-HH Sitzung from the intermediate media file.

The mediathek session page provides the per-speech spine (speaker, role, faction,
per-TOP agenda title, the speech's own server-cut HLS clip and its wall-clock
times). Each speech's ``videoFileURI`` is that clip (per-speech video file mode),
with ``media.duration`` and ``additionalInformation.startOffset = 0`` /
``endOffset = duration``; the sign-language clip of the same range is in
``additionalInformation.signLanguageVideoFileURI``. ``sourcePage`` is the
mediathek's own per-speech anchor (``…#rede-{speechPk}``), unique per speech as
the platform keys speech identity on it; the source's speech UUID is
``media.originMediaID`` (there is no joint media+proceedings id, so no speech
``originID``). The speech ``dateStart``/``dateEnd`` are **real wall-clock** times
(from the per-speech video-download timestamps), so
``debug.timesAreVideoRelative`` is ``false``.

Text, one source per sitting:

- once the Plenarprotokoll PDF (ParlDok) is parsed via ``optv.shared.pdf2tei``,
  it is joined onto the spine (``join_text_to_spine``, ``type: "proceedings"``,
  untimed until aeneas); speeches with no text match keep ``textContents: []``.
  This join is **experimental and unvalidated** — see manifest;
- before that, each speech with mediathek subtitles carries them as
  ``type: "generated"`` text whose sentences are already timed against the
  speech clip (``debug.textSource = "subtitles"``); the align stage then only
  stamps them (``workflow.py``).
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from optv.shared.agenda_types import classify_de_hh
from optv.shared.merge_format import agenda_id as _agenda_id
from optv.shared.lang.de import speaker_context as _speaker_context

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from optv.parliaments import get_rights as _get_rights
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import load_turns, join_text_to_spine
from optv.shared.meta import build_meta, now_iso

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-HH"
SOURCE_URI = _get_rights("DE-HH", stream="media")["sourceURI"]
PROCEEDINGS_CREATOR = _get_rights("DE-HH", stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights("DE-HH", stream="proceedings")["license"]
TRANSCRIPT_CREATOR = _get_rights("DE-HH", stream="transcript")["creator"]
TRANSCRIPT_LICENSE = _get_rights("DE-HH", stream="transcript")["license"]


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
    duration = float(speech.get("duration") or 0)
    page = speech.get("video_page_url") or SOURCE_URI
    pk = speech.get("speech_pk") or ""
    media: dict = {
        "videoFileURI": speech.get("video_uri") or "",
        "duration": duration,
        # sourcePage must be unique per speech (the platform keys speech
        # identity on it); the mediathek anchors each speech at #rede-{pk}.
        "sourcePage": f"{page}#rede-{pk}" if pk else page,
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": speech.get("speech_id") or pk,
    }
    # The clip is the speech: it spans 0..duration of the file.
    extras: dict = {"startOffset": 0.0, "endOffset": duration}
    sign = speech.get("sign_video_uri") or ""
    if sign:
        extras["signLanguageVideoFileURI"] = sign
    media["additionalInformation"] = extras
    return media


def _attach_subtitles(merged: list[dict], speeches: list[dict]) -> int:
    """Put each speech's subtitle sentences (timed against its clip) into
    ``textContents`` as ``generated`` text. Returns the number of speeches."""
    attached = 0
    for rec, sp in zip(merged, speeches):
        sentences = sp.get("subtitle_sentences")
        if not sentences:
            continue
        first = (rec.get("people") or [{}])[0]
        rec["textContents"] = [{
            "type": "generated",
            "language": "de",
            "creator": TRANSCRIPT_CREATOR,
            "license": TRANSCRIPT_LICENSE,
            "textBody": [{
                "type": "speech",
                "speaker": first.get("label") or "",
                "speakerstatus": first.get("context") or "main-speaker",
                "sentences": sentences,
            }],
        }]
        dbg = rec.setdefault("debug", {})
        dbg["textSource"] = "subtitles"
        # The speech's own subtitle track is its text record (as DE-NI's VTT).
        dbg["proceedingIndex"] = rec.get("speechIndex")
        # Cut by the speech's own time range, not joined by speaker order.
        dbg["confidence"] = 1.0
        attached += 1
    return attached


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
            "originalLanguage": "de",
            "agendaItem": _build_agenda(sp),
            "people": [_build_person(sp)],
            "media": _build_media(sp),
            "textContents": [],
            "documents": [],
            "debug": {
                "source": "mediathek-sessionpage",
                "sessionUUID": sp.get("session_uuid"),
                "speechLegacyId": sp.get("legacy_id"),
                "topIndex": sp.get("top_index"),
                "topNumber": sp.get("top_number"),
                "rednerRaw": sp.get("name_raw"),
                "functionRaw": sp.get("function_raw"),
                "roleRaw": sp.get("role"),
                # Offsets within the agenda item's clip.
                "startOffset": sp.get("start_offset"),
                "endOffset": sp.get("end_offset"),
                "duration": sp.get("duration"),
                # Per-speech dateStart/dateEnd are real wall-clock (from the
                # mediathek video-download timestamps), not video-relative.
                "timesAreVideoRelative": False,
            },
        }
        merged.append(record)

    # Text: the protocol (if parsed) joined onto the fixed media spine, else the
    # mediathek subtitles.
    turns = load_turns(config, session)
    if turns:
        spine_keys = [_match_key(sp.get("label") or sp.get("name_raw") or "") for sp in speeches]
        matched = join_text_to_spine(merged, spine_keys, turns,
                                     creator=PROCEEDINGS_CREATOR,
                                     license=PROCEEDINGS_LICENSE)
        logger.info(f"{session}: matched {matched}/{len(merged)} speeches to "
                    f"{len(turns)} proceedings turns")
    else:
        attached = _attach_subtitles(merged, speeches)
        if attached:
            logger.info(f"{session}: subtitle text for {attached}/{len(merged)} speeches")

    doc = {
        "meta": build_meta(
            PARLIAMENT_ID,
            session=session,
            electoral_period=wp,
            date_start=earliest,
            date_end=latest,
            processing={
                **media_doc["meta"].get("processing", {}),
                "merge": now_iso(),
            },
            extra={"sourceURI": media_doc["meta"].get("video_page_url") or SOURCE_URI},
        ),
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
