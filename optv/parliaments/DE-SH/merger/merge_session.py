#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-SH Sitzung from the intermediate media file.

The m7k mediathek's ``result.php`` provides the per-speech spine (IDs, speaker,
faction, agenda title and ``#t=start,end`` video offsets). The Plenarprotokoll PDF
is parsed via ``optv.shared.pdf2tei`` and joined onto that spine here
(``join_text_to_spine``), so each matched speech carries verbatim ``textContents``
(speeches with no text match keep ``textContents: []``). This text join is
**experimental and unvalidated** — see manifest.

The list's clock times are German local time; ``dateStart``/``dateEnd`` carry
their Europe/Berlin UTC offset (``debug.timesAreVideoRelative = False``), and
``session.dateStart``/``dateEnd`` span the whole sitting.

Agenda: m7k's short ``thema`` is the title. For a speech whose ``thema`` is
empty or reads "Präsidium", the title comes from another speech of the same TOP,
else from the protocol's agenda title of the matched text turn (PDF bookmarks),
else ``TOP n``; such a speech without a TOP takes the TOP of its neighbours when
both have the same one. ``debug.agendaSource`` records which.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from optv.shared.agenda_types import classify_de_sh
from optv.shared.merge_format import split_first_last as _split_first_last
from optv.shared.lang.de import strip_honorifics as _strip_honorifics
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import (
    assign_join_confidence,
    join_text_to_spine,
    load_turns,
)

from optv.parliaments import get_rights as _get_rights
from optv.shared.meta import build_meta, now_iso
from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE
from ..scraper.common import M7K_BASE

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-SH"
PROCEEDINGS_CREATOR = _get_rights(PARLIAMENT_ID, stream="proceedings")["creator"]
PROCEEDINGS_LICENSE = _get_rights(PARLIAMENT_ID, stream="proceedings")["license"]
SOURCE_PAGE_TEMPLATE = M7K_BASE + "/iframe.php?b={speech_id}"
BERLIN = ZoneInfo("Europe/Berlin")

# m7k ``thema`` values that name no agenda item: empty (``class="result crap"``)
# and "Präsidium", which m7k also puts on some members' speeches in a debate.
_NO_TOPIC = {"", "präsidium"}

# Map the m7k ``gruppe`` field to (faction_label_or_None, speakerContext).
# Most values are bare party shortcodes (CDU, SPD, FDP, Grüne, SSW). The
# ``Regierung`` marker is what m7k attaches to cabinet members speaking
# from the government bench — no party, but a recognised role.
_GROUPS_NO_FACTION = {"regierung", "präsidium", "praesidium", "alterspräsidium"}


def _speaker_context_from_gruppe(gruppe: str, redner: str) -> str:
    g = (gruppe or "").strip().lower()
    name = (redner or "").lower()
    if g == "regierung":
        return "main-speaker"
    if g in _GROUPS_NO_FACTION:
        # Presidial role — try to refine via the redner string.
        if "alterspräsident" in name:
            return "interim-president"
        if "vizepräsident" in name:
            return "vice-president"
        if "präsident" in name:
            return "president"
        return "main-speaker"
    # Try the redner prefix even when gruppe carries a party — chairs
    # speaking pro-tempore still get a presidial context.
    if "alterspräsident" in name:
        return "interim-president"
    if "vizepräsident" in name:
        return "vice-president"
    if "präsident" in name and "ministerpräsident" not in name:
        return "president"
    return "main-speaker"


_NAME_ROLE_RE = re.compile(
    r'^\s*(?P<role>'
    r'Pr[äa]sident(?:in)?|Vizepr[äa]sident(?:in)?|Alterspr[äa]sident(?:in)?|'
    r'Ministerpr[äa]sident(?:in)?|Minister(?:in)?|Staatssekret[äa]r(?:in)?|'
    r'Staatsminister(?:in)?'
    r')\s+(?P<rest>.+?)\s*$',
    re.UNICODE,
)


def _split_role_and_name(redner: str) -> tuple[str | None, str]:
    """``"Ministerin Karin Prien"`` → (``"Ministerin"``, ``"Karin Prien"``)."""
    if not redner:
        return None, redner
    m = _NAME_ROLE_RE.match(redner.strip())
    if not m:
        return None, redner.strip()
    return m.group("role"), m.group("rest").strip()


def _build_person(speech: dict) -> dict:
    role, plain_name = _split_role_and_name(speech.get("redner", ""))
    plain_name = _strip_honorifics(plain_name)
    context = _speaker_context_from_gruppe(
        speech.get("gruppe", ""), speech.get("redner", "")
    )
    person: dict = {
        "label": plain_name or speech.get("redner", "") or "Unbekannt",
        "context": context,
    }
    first, last = _split_first_last(plain_name)
    if first:
        person["firstname"] = first
    if last:
        person["lastname"] = last
    if role:
        person["role"] = role

    gruppe = (speech.get("gruppe") or "").strip()
    if gruppe and gruppe.lower() not in _GROUPS_NO_FACTION:
        person["faction"] = {"label": gruppe}
    return person


def _has_topic(speech: dict) -> bool:
    return (speech.get("thema") or "").strip().casefold() not in _NO_TOPIC


def _resolve_m7k_agenda(speeches: list[dict]) -> list[list]:
    """``[top, title, source]`` per speech from the m7k list alone.

    ``speeches`` is one sitting in ``beginn`` order. A speech without a topic
    keeps its TOP; without one it takes the TOP of the nearest preceding and
    following speeches that have one, if both agree. Its title is the ``thema``
    of another speech with the same TOP (``m7k-sibling``); otherwise title and
    source stay empty for :func:`_fill_titles_from_proceedings`.
    """
    tops = [(sp.get("top") or "").strip() for sp in speeches]
    thema_by_top: dict[str, str] = {}
    for sp, top in zip(speeches, tops):
        if top and _has_topic(sp):
            thema_by_top.setdefault(top, sp["thema"].strip())
    out: list[list] = []
    for i, sp in enumerate(speeches):
        top = tops[i]
        if _has_topic(sp):
            out.append([top, sp["thema"].strip(), "m7k"])
            continue
        if not top:
            prev = next((t for t in reversed(tops[:i]) if t), "")
            nxt = next((t for t in tops[i + 1:] if t), "")
            if prev and prev == nxt:
                top = prev
        if top in thema_by_top:
            out.append([top, thema_by_top[top], "m7k-sibling"])
        else:
            out.append([top, "", None])
    return out


def _fill_titles_from_proceedings(merged: list[dict], agenda: list[list],
                                  turns: list[dict]) -> None:
    """Title the still-untitled entries of ``agenda`` from the protocol.

    A matched record's text turn carries the protocol's agenda title. Records of
    one TOP share the most common title among that TOP's matched turns, so an
    unmatched record of the TOP gets it too; a record without a TOP uses its own
    turn's title.
    """
    by_index = {t.get("index"): t for t in turns}

    def turn_title(rec: dict) -> str:
        idx = (rec.get("debug") or {}).get("proceedingIndex")
        turn = by_index.get(idx) if idx is not None else None
        return ((turn or {}).get("agendaTitle") or "").strip()

    per_top: dict[str, Counter] = {}
    for rec, (top, title, source) in zip(merged, agenda):
        if top and not source and (t := turn_title(rec)):
            per_top.setdefault(top, Counter())[t] += 1
    for rec, entry in zip(merged, agenda):
        top, title, source = entry
        if source:
            continue
        if top:
            t = per_top[top].most_common(1)[0][0] if top in per_top else ""
        else:
            t = turn_title(rec)
        if t:
            entry[1], entry[2] = t, "proceedings"


def _build_agenda(top: str, title: str) -> dict:
    top = (top or "").strip()
    title = (title or "").strip()
    label = title or (f"TOP {top}" if top else "")
    official_title = f"TOP {top}: {title}" if (top and title) else label
    out: dict = {
        "officialTitle": official_title or "(ohne Titel)",
        "title": label or "(ohne Titel)",
    }
    if top:
        out["id"] = f"TOP-{top}"
    native, core = classify_de_sh(title)
    if native:
        out["nativeType"] = native
    out["type"] = core
    return out


def _build_media(speech: dict, session_date: str) -> dict:
    video_uri = speech.get("videoFileURI") or ""
    sourcepage = SOURCE_PAGE_TEMPLATE.format(speech_id=speech["speech_id"])
    media: dict = {
        "videoFileURI": video_uri,
        "sourcePage": sourcepage,
        "creator": MEDIA_CREATOR,
        "license": MEDIA_LICENSE,
        "originMediaID": speech["speech_id"],
    }
    duration = int(speech.get("duration_seconds") or 0)
    if duration:
        media["duration"] = duration
    extras: dict = {}
    if speech.get("videoFileURI_webm"):
        extras["videoFileURI_webm"] = speech["videoFileURI_webm"]
    beginn = speech.get("beginn") or ""
    ende = speech.get("ende") or ""
    if beginn or ende:
        extras["clockBeginn"] = beginn
        extras["clockEnde"] = ende
    if extras:
        media["additionalInformation"] = extras
    return media


def _local_datetime(date: str, hms: str) -> datetime | None:
    """``("2026-10-09", "10:06:09")`` → aware datetime in Europe/Berlin."""
    if not (hms or "").strip():
        return None
    try:
        naive = datetime.strptime(f"{date}T{hms.strip()}", "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
    return naive.replace(tzinfo=BERLIN)


def _iso(dt: datetime) -> str:
    return dt.isoformat("T", "seconds")


def _speech_span(session_date: str, speech: dict) -> tuple[datetime, datetime]:
    """Start/end of one speech from the list's clock times.

    A missing ``beginn`` falls back to midnight of the sitting day (keeps sorts
    deterministic), a missing ``ende`` to start + duration; an ``ende`` before
    ``beginn`` crossed midnight.
    """
    start = _local_datetime(session_date, speech.get("beginn", ""))
    if start is None:
        start = datetime.fromisoformat(session_date).replace(tzinfo=BERLIN)
    end = _local_datetime(session_date, speech.get("ende", ""))
    if end is None:
        end = start + timedelta(seconds=max(int(speech.get("duration_seconds") or 0), 1))
    elif end < start:
        end += timedelta(days=1)
    return start, end


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
    tagung_no = media_doc["meta"].get("tagung")

    spans = [_speech_span(session_date, sp) for sp in speeches]
    earliest_iso = _iso(min(start for start, _ in spans))
    latest_iso = _iso(max(end for _, end in spans))
    agenda = _resolve_m7k_agenda(speeches)

    merged: list[dict] = []
    for sp, (start, end), (top, title, _source) in zip(speeches, spans, agenda):
        record: dict = {
            "parliament": PARLIAMENT_ID,
            "electoralPeriod": {"number": wp},
            "session": {
                "number": sitzung_no,
                "dateStart": earliest_iso,
                "dateEnd": latest_iso,
            },
            "dateStart": _iso(start),
            "dateEnd": _iso(end),
            "speechIndex": sp.get("speech_index") or 0,
            "originID": sp["speech_id"],
            "originalLanguage": "de",
            "agendaItem": _build_agenda(top, title),
            "people": [_build_person(sp)],
            "media": _build_media(sp, session_date),
            "textContents": [],
            "documents": [],
            "debug": {
                "source": "m7k",
                "tagung": tagung_no,
                "topNo": sp.get("top"),
                "themaRaw": sp.get("thema"),
                "rednerRaw": sp.get("redner"),
                "gruppeRaw": sp.get("gruppe"),
                "beginnClock": sp.get("beginn"),
                "endeClock": sp.get("ende"),
                "durationSeconds": sp.get("duration_seconds"),
                "timesAreVideoRelative": False,
            },
        }
        merged.append(record)

    # Spine-join: attach proceedings text (if parsed) onto the fixed media spine.
    turns = load_turns(config, session)
    if turns:
        spine_keys = [_match_key(sp.get("redner") or "") for sp in speeches]
        matched = join_text_to_spine(merged, spine_keys, turns,
                                     creator=PROCEEDINGS_CREATOR,
                                     license=PROCEEDINGS_LICENSE)
        logger.info(f"{session}: matched {matched}/{len(merged)} speeches to "
                    f"{len(turns)} proceedings turns")

    # Agenda items m7k leaves untitled: protocol title, else "TOP n".
    untitled = [entry[2] is None for entry in agenda]
    if any(untitled):
        _fill_titles_from_proceedings(merged, agenda, turns)
        for rec, (top, title, source), was_untitled in zip(merged, agenda, untitled):
            if was_untitled:
                rec["agendaItem"] = _build_agenda(top, title)
        if turns:
            # The join gated confidence on the provisional agenda type.
            assign_join_confidence(merged)
    for rec, entry in zip(merged, agenda):
        rec["debug"]["agendaSource"] = entry[2] or ("top-number" if entry[0] else "none")

    doc = {
        "meta": build_meta(
            PARLIAMENT_ID,
            session=session,
            electoral_period=wp,
            date_start=earliest_iso,
            date_end=latest_iso,
            processing={
                **media_doc["meta"].get("processing", {}),
                "merge": now_iso(),
            },
            extra={"tagung": tagung_no, "sourceURI": f"{M7K_BASE}/"},
        ),
        "data": merged,
    }
    return config.save_data(doc, session, "merged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session", help="Session ID e.g. 20119")
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
