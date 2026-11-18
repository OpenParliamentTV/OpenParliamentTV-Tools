#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-NW Plenarsitzung from the intermediate media file.

The mediathek session page provides the per-speech spine (speaker, MdL id, role,
faction, per-TOP agenda title and a precise start offset). The HTML
Plenarprotokoll (``MMP18-{N}.html``, parsed by ``parsers/proceedings2json.py``
into one rede per spine entry) is joined onto that spine here, per agenda item:
the spine's ``topNr`` and the protocol's numbered agenda headings group both
sides, and ``join_text_to_spine`` matches surnames within each group. Matched
speeches carry verbatim ``textContents`` (speeches with no text match keep
``textContents: []``). This text join is **experimental and unvalidated** — see
manifest.

DE-NW is the SE/DE-SH per-speech-offset model: one HLS stream per session and
per-speech windows addressed by an HTML5 media-fragment ``#t=start,end`` on
``videoFileURI`` (offsets into that one stream) plus ``startOffset``/``endOffset``
in ``additionalInformation``. ``sourcePage`` is the session page with the
per-speech ``&top-redner-id={id}`` query — already unique per speech, so the
platform's sourcePage-keyed speech identity does not collapse distinct speeches.
The speech ``dateStart``/``dateEnd`` are **real wall-clock** (session start from
the page header, localised to Europe/Berlin, + offset), so
``debug.timesAreVideoRelative`` is ``false``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from optv.shared.agenda_types import classify_de_nw
from optv.shared.merge_format import agenda_id, format_offset as _num
from optv.shared.lang.de import speaker_context as _speaker_context

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from ..scraper.common import hls_url, video_page_url
from optv.parliaments import get_rights as _get_rights
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import load_turns, join_text_to_spine
from optv.shared.cli import session_cli
from optv.shared.spine_merger import load_spine, save_merged

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-NW"
SOURCE_URI = _get_rights("DE-NW", stream="media")["sourceURI"]
PROCEEDINGS_CREATOR = _get_rights("DE-NW", stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights("DE-NW", stream="proceedings")["license"]


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
        "id": agenda_id(top_number, title, fallback="eroeffnung"),
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


def _top_key(value) -> str:
    return str(value or "").strip().lower()


def join_text_by_agenda_item(merged: list[dict], speeches: list[dict],
                             turns: list[dict]) -> int:
    """Join proceedings turns onto the spine within each agenda item.

    Spine speeches are grouped by their ``top_nr`` and turns by ``topNumber``
    (the opening of the sitting has neither, key ``""``); ``join_text_to_spine``
    then aligns surnames inside each group, so text never crosses into another
    agenda item. Returns the number of matched speeches.
    """
    spine_groups: dict[str, list[int]] = {}
    for i, sp in enumerate(speeches):
        spine_groups.setdefault(_top_key(sp.get("top_nr")), []).append(i)
    turn_groups: dict[str, list[dict]] = {}
    for t in turns:
        turn_groups.setdefault(_top_key(t.get("topNumber")), []).append(t)
    matched = 0
    for top, idx in spine_groups.items():
        group_turns = turn_groups.get(top)
        if not group_turns:
            logger.debug(f"agenda item {top or '(opening)'}: no protocol text")
            continue
        keys = [_match_key(speeches[i].get("label") or speeches[i].get("name_raw") or "")
                for i in idx]
        matched += join_text_to_spine([merged[i] for i in idx], keys, group_turns,
                                      creator=PROCEEDINGS_CREATOR,
                                      license=PROCEEDINGS_LICENSE)
    return matched


def merge_session(session: str, config, options=None) -> Path:
    spine = load_spine(config, session)
    if spine is None:
        return config.file(session, "merged", create=True)
    media_doc, speeches = spine

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

    # Spine-join: attach proceedings text (if parsed) onto the fixed media spine.
    turns = load_turns(config, session)
    if turns:
        matched = join_text_by_agenda_item(merged, speeches, turns)
        logger.info(f"{session}: matched {matched}/{len(merged)} speeches to "
                    f"{len(turns)} proceedings turns")

    return save_merged(
        config, session, PARLIAMENT_ID, merged, media_doc,
        electoral_period=wp, date_start=earliest, date_end=latest,
        extra={"sourceURI": meta.get("video_page_url") or SOURCE_URI},
    )


if __name__ == "__main__":
    session_cli(PARLIAMENT_ID, merge_session, description=__doc__, example="18117")
