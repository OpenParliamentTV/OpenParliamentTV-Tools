#! /usr/bin/env python3
"""Emit Stage 2 JSON for one DE-SH Sitzung from the intermediate media file.

There is no proceedings stream to merge — Plenarprotokolle are PDF-only
and no parser exists yet (see manifest ``supported_stages``). The m7k
mediathek's ``result.php`` already provides per-speech segmentation with
IDs, speaker, faction, agenda title and ``#t=start,end`` video offsets,
so the "merge" here is really a translation pass from the intermediate
shape into Stage 2 with ``textContents: []``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-SH.merger"

from optv.shared.agenda_types import classify_de_sh
from optv.shared.merge_format import split_first_last as _split_first_last
from optv.shared.lang.de import strip_honorifics as _strip_honorifics
from optv.shared.lang.de import match_key_surname as _match_key
from optv.shared.pdf2tei.spine_join import load_turns, join_text_to_spine

from ..parsers.media2json import MEDIA_CREATOR, MEDIA_LICENSE

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-SH"
PROCEEDINGS_CREATOR = "Schleswig-Holsteinischer Landtag"
PROCEEDINGS_LICENSE = "Amtliches Werk (§ 5 Abs. 2 UrhG)"
SOURCE_PAGE_TEMPLATE = "https://m7k.ltsh.de/iframe.php?b={speech_id}"

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


# Honorifics to strip from the name before NEL matches the entity dump.
# Wikidata MP labels don't carry "Dr."/"Prof." prefixes so leaving them on
# the person.label costs ~40% of the wid hit rate.
_HONORIFICS = ("Dr. ", "Prof. ", "Prof. Dr. ", "Dr. Dr. ", "Dr. h. c. ")


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


def _build_agenda(speech: dict) -> dict:
    thema = (speech.get("thema") or "").strip()
    top = (speech.get("top") or "").strip()
    title = thema or (f"TOP {top}" if top else "")
    official_title = title
    if top and thema:
        official_title = f"TOP {top}: {thema}"
    out: dict = {
        "officialTitle": official_title or title or "(ohne Titel)",
        "title": title or "(ohne Titel)",
    }
    if top:
        out["id"] = f"TOP-{top}"
    native, core = classify_de_sh(thema)
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


def _to_iso_datetime(date: str, hms: str) -> str | None:
    if not hms:
        return None
    try:
        return f"{date}T{hms}"
    except Exception:
        return None


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

    merged: list[dict] = []
    earliest_iso: str | None = None
    latest_iso: str | None = None

    for sp in speeches:
        date_start = _to_iso_datetime(session_date, sp.get("beginn", ""))
        date_end = _to_iso_datetime(session_date, sp.get("ende", ""))
        if date_start is None:
            # Synthesise a sequential timestamp so downstream sorts remain
            # deterministic when m7k drops a beginn time.
            date_start = f"{session_date}T00:00:00"
        if date_end is None:
            duration = int(sp.get("duration_seconds") or 0)
            try:
                dt0 = datetime.fromisoformat(date_start)
                date_end = (dt0 + timedelta(seconds=max(duration, 1))).isoformat("T", "seconds")
            except ValueError:
                date_end = date_start
        earliest_iso = min(earliest_iso, date_start) if earliest_iso else date_start
        latest_iso = max(latest_iso, date_end) if latest_iso else date_end

        origin_id = sp["speech_id"]
        speech_index = sp.get("speech_index") or 0

        record: dict = {
            "parliament": PARLIAMENT_ID,
            "electoralPeriod": {"number": wp},
            "session": {
                "number": sitzung_no,
                "dateStart": _to_iso_datetime(session_date, sp.get("beginn", "")) or f"{session_date}T00:00:00",
                "dateEnd": _to_iso_datetime(session_date, sp.get("ende", "")) or f"{session_date}T23:59:59",
            },
            "dateStart": date_start,
            "dateEnd": date_end,
            "speechIndex": speech_index,
            "originID": origin_id,
            "originalLanguage": "de",
            "agendaItem": _build_agenda(sp),
            "people": [_build_person(sp)],
            "media": _build_media(sp, session_date),
            "textContents": [],
            "documents": [],
            "debug": {
                "source": "m7k",
                "tagung": tagung_no,
                "topNo": sp.get("top"),
                "rednerRaw": sp.get("redner"),
                "gruppeRaw": sp.get("gruppe"),
                "beginnClock": sp.get("beginn"),
                "endeClock": sp.get("ende"),
                "durationSeconds": sp.get("duration_seconds"),
            },
        }
        merged.append(record)

    earliest_iso = earliest_iso or f"{session_date}T00:00:00"
    latest_iso = latest_iso or f"{session_date}T23:59:59"

    # Spine-join: attach proceedings text (if parsed) onto the fixed media spine.
    turns = load_turns(config, session)
    if turns:
        spine_keys = [_match_key(sp.get("redner") or "") for sp in speeches]
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
            "tagung": tagung_no,
            "dateStart": earliest_iso,
            "dateEnd": latest_iso,
            "sourceURI": f"https://m7k.ltsh.de/",
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
