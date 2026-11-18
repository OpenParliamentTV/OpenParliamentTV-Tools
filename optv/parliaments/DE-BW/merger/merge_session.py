#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-BW Sitzung from the intermediate media file.

The mediathek chapter list provides the per-speech spine (speaker, role, faction,
agenda (TOP) title and a start offset into the one session MP4). The
Plenarprotokoll PDF is parsed via ``optv.shared.pdf2tei`` and joined onto that
spine here (``join_text_to_spine``), so each matched speech carries verbatim
``textContents`` (speeches with no text match keep ``textContents: []``). This
text join is **experimental and unvalidated** — see manifest.

DE-BW is the SE/DE-SH per-speech-offset model: one session recording, per-speech
windows addressed by an HTML5 media-fragment ``#t=start,end`` on
``videoFileURI`` plus a ``startOffset`` in ``additionalInformation``. The
``sourcePage`` is made unique per speech (``…?t=start``, the query the page's
player reads to start at a position) so the platform's sourcePage-keyed speech
identity does not collapse distinct speeches.

Speech times: the chapter list has offsets only, but the protocol states the
sitting's start, breaks and end to the minute (``meta.sittingTimes`` of the
parsed proceedings). Part 1 of the recording starts at the sitting's start and
each further part at the resumption after the break that ended the previous
part, so ``dateStart`` = part start + offset, in Europe/Berlin time
(``debug.timesAreVideoRelative = false``). Until the protocol is published, or
when its times don't fit the parts, the parser's synthetic offsets from
midnight stay (naive, ``debug.timesAreVideoRelative = true``).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from optv.shared.agenda_types import classify_de_bw

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from optv.parliaments import get_rights as _get_rights
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import (
    load_turns, join_text_to_spine, load_proceedings_meta,
)
from optv.shared.meta import build_meta, now_iso

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-BW"
SOURCE_URI = _get_rights("DE-BW", stream="media")["sourceURI"]
PROCEEDINGS_CREATOR = _get_rights("DE-BW", stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights("DE-BW", stream="proceedings")["license"]

BERLIN = ZoneInfo("Europe/Berlin")
# Protocol times are to the minute, chapter offsets to the second.
_CLOCK_SLACK = timedelta(seconds=120)

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
        # identity on it); the page is one URL per session part, so append
        # the per-speech offset as ``?t=`` — the page's player reads it on
        # load and starts at the speech.
        "sourcePage": f"{page}?t={start}",
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


def _clock(day: str, hm: str) -> datetime:
    """``("2026-10-08", "09:33")`` → Europe/Berlin-aware datetime."""
    h, m = hm.split(":")
    return datetime.combine(datetime.strptime(day, "%Y-%m-%d").date(),
                            time(int(h), int(m)), tzinfo=BERLIN)


def _part_anchors(speeches: list[dict], times: dict,
                  day: str) -> dict[int, tuple[datetime, datetime]] | None:
    """Map each video part to its wall-clock ``(start, end)`` from the protocol.

    Part 1 starts at the sitting's ``begin``. Each further part starts at the
    resumption after the first break that begins once the previous part's
    last chapter entry has started (minus the minute-resolution slack): a short
    break inside a part (e.g. counting a roll-call vote) does not split the
    recording, so it is skipped. The break's interruption ends the previous
    part; the last part ends at ``end``. Returns ``None`` (caller falls back to
    video-relative times) when the parts are not numbered 1..n, a part
    boundary has no matching break, or the last part's entries overrun ``end``.
    """
    last_start: dict[int, int] = {}
    for sp in speeches:
        p = int(sp["part"])
        last_start[p] = max(last_start.get(p, 0), int(sp["start_offset"]))
    parts = sorted(last_start)
    if parts != list(range(1, len(parts) + 1)):
        return None
    try:
        begin = _clock(day, times["begin"])
        end = _clock(day, times["end"])
        breaks = sorted((_clock(day, a), _clock(day, b))
                        for a, b in times.get("breaks") or [])
    except (KeyError, ValueError, TypeError):
        return None

    anchors: dict[int, tuple[datetime, datetime]] = {}
    start = begin
    for i, p in enumerate(parts):
        reach = start + timedelta(seconds=last_start[p])
        if i + 1 < len(parts):
            nxt = next(((u, w) for u, w in breaks if u >= reach - _CLOCK_SLACK), None)
            if nxt is None:
                return None
            part_end, next_start = nxt
        else:
            if reach > end + _CLOCK_SLACK:
                return None
            part_end, next_start = end, None
        anchors[p] = (start, part_end)
        start = next_start
    return anchors


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _wall_clock(speeches: list[dict], times: dict | None,
                day: str) -> tuple[list[tuple[str, str]], str, str,
                                   dict[int, str]] | None:
    """Wall-clock ``(dateStart, dateEnd)`` per speech + the session span.

    Speech start = its part's start + chapter offset; end = the next entry's
    start in the same part, or the part's end (break or ``Schluss``) for the
    last entry. Returns ``(per_speech, session_start, session_end,
    part_start_iso)`` or ``None`` when the protocol times are missing or don't
    fit the parts.
    """
    if not times:
        return None
    anchors = _part_anchors(speeches, times, day)
    if anchors is None:
        return None
    per_speech: list[tuple[str, str]] = []
    first, last = _clock(day, times["begin"]), _clock(day, times["end"])
    for sp in speeches:
        start, part_end = anchors[int(sp["part"])]
        ds = start + timedelta(seconds=int(sp["start_offset"]))
        if sp.get("end_offset") is not None:
            de = start + timedelta(seconds=int(sp["end_offset"]))
        else:
            de = max(part_end, ds)
        first, last = min(first, ds), max(last, de)
        per_speech.append((_iso(ds), _iso(de)))
    part_starts = {p: _iso(a[0]) for p, a in anchors.items()}
    return per_speech, _iso(first), _iso(last), part_starts


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

    # Wall clock from the protocol's sitting times when the protocol is in and
    # its times fit the video parts; else the parser's synthetic,
    # video-relative times (naive, flagged in debug).
    times = load_proceedings_meta(config, session).get("sittingTimes")
    clock = _wall_clock(speeches, times, session_date)
    if times and clock is None:
        logger.warning(f"{session}: protocol sitting times {times} do not fit the "
                       f"video parts — keeping video-relative times")
    if clock is not None:
        per_speech, earliest, latest, part_starts = clock
    else:
        per_speech, part_starts = None, {}
        starts = [s.get("start_datetime") for s in speeches if s.get("start_datetime")]
        ends = [s.get("end_datetime") for s in speeches if s.get("end_datetime")]
        earliest = min(starts) if starts else f"{session_date}T00:00:00"
        latest = max(ends) if ends else f"{session_date}T23:59:59"

    merged: list[dict] = []
    for i, sp in enumerate(speeches):
        if per_speech is not None:
            date_start, date_end = per_speech[i]
        else:
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
                # The chapter list has offsets only. With the protocol's
                # sitting times the speech times are wall clock (part start +
                # offset); without, they are synthetic offsets from midnight.
                "timesAreVideoRelative": clock is None,
                "timeSource": "protocol" if clock is not None else "video",
            },
        }
        if sp.get("part") in part_starts:
            record["debug"]["partStart"] = part_starts[sp["part"]]
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
            extra={"sourceURI": SOURCE_URI},
        ),
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
