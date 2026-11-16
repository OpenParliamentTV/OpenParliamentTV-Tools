#! /usr/bin/env python3

# Convert the per-session raw interventions file (written by
# scraper/fetch_interventions.py) into the intermediate "media" JSON stream:
# one item per video clip (usually one per speech), carrying the video,
# speaker, agenda and (coarse, HH:MM) timing. The proceedings stream supplies the transcript text;
# the merger joins the two by session + speaker surname.

import logging
logger = logging.getLogger(__name__)

import argparse
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

_repo_root = str(Path(__file__).resolve().parents[4])
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from optv.parliaments.ES.parsers.common import (
    collapse_ws, intervention_page_url, parse_orador, parse_video_url, video_urls,
)
from optv.shared.agenda_types import annotate_agenda_item, classify_es
from optv.parliaments import get_rights as _get_rights

MADRID = ZoneInfo("Europe/Madrid")
PARLIAMENT = "ES"
# Single Aviso Legal covers both video and proceedings reuse.
MEDIA_LICENSE = _get_rights("ES", stream="media")["license"]
MEDIA_CREATOR = _get_rights("ES", stream="media")["creator"]

# The open data links the MP4s on www.congreso.es, which refuses non-browser
# clients; static.congreso.es serves the same paths without that restriction
# and is the host the audiovisual archive's sitting pages link.
_WWW_VIDEO_PREFIX = "https://www.congreso.es/audiovisual/"
_STATIC_VIDEO_PREFIX = "https://static.congreso.es/audiovisual/"


def _video_file_uri(mp4: str) -> str:
    mp4 = mp4.strip()
    if mp4.startswith(_WWW_VIDEO_PREFIX):
        return _STATIC_VIDEO_PREFIX + mp4[len(_WWW_VIDEO_PREFIX):]
    return mp4


def _parse_dt(sesion: str, hhmm: str):
    """Combine SESION (dd/mm/yyyy) + HH:MM into a Europe/Madrid aware datetime.

    Returns None if either part is missing/unparseable.
    """
    if not sesion:
        return None
    try:
        day, month, year = (int(x) for x in sesion.strip().split("/"))
    except ValueError:
        return None
    hour, minute = 0, 0
    have_time = False
    if hhmm and ":" in hhmm:
        try:
            hour, minute = (int(x) for x in hhmm.strip().split(":")[:2])
            have_time = True
        except ValueError:
            have_time = False
    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=MADRID)
    except ValueError:
        return None
    return dt, have_time


def parse_intervention(rec: dict, period: int, number: int) -> list:
    """Convert one raw intervention record into media-stream items, one per clip.

    A record usually has one MP4. An intervention recorded in several clips
    (e.g. a minister's answer and rejoinder around the questioner's reply)
    lists them all; each clip becomes its own item, flagged with
    `debug.clipIndex` / `debug.clipCount`, sharing the record's speaker,
    agenda and times. Returns [] for records the media stream cannot
    represent (placeholder URL without a video, or no usable date).
    """
    clips = [(url, video) for url in video_urls(rec.get("ENLACEDESCARGADIRECTA"))
             if (video := parse_video_url(url))]
    if not clips:
        # No per-speech video — not a media-stream item. The proceedings
        # stream still carries this speaker's text into the merge.
        return []

    sesion = rec.get("SESION", "")
    start = _parse_dt(sesion, rec.get("INICIOINTERVENCION", ""))
    end = _parse_dt(sesion, rec.get("FININTERVENCION", ""))

    debug: dict = {}
    if start is None:
        # No usable date — caller will drop.
        return []
    start_dt, have_start_time = start
    if not have_start_time:
        debug["no-start-time"] = True
    if end is not None:
        end_dt, have_end_time = end
        if end_dt < start_dt:
            end_dt = start_dt
    else:
        end_dt, have_end_time = start_dt, False
    duration = (end_dt - start_dt).total_seconds()
    # HH:MM resolution only — flag it so downstream knows timing is coarse.
    debug["coarseTiming"] = True

    objeto = collapse_ws(rec.get("OBJETOINICIATIVA"))
    fase = collapse_ws(rec.get("FASE"))
    title = objeto or fase or "Sesión plenaria"
    agenda = {
        "title": title,
        "officialTitle": objeto or fase or title,
    }
    nt, ct = classify_es(objeto, fase, rec.get("TIPOINTERVENCION"))
    annotate_agenda_item(agenda, nt, ct)

    person = parse_orador(rec.get("ORADOR", ""), rec.get("CARGOORADOR", ""))

    items = []
    for k, (url, video) in enumerate(clips):
        item_debug = dict(debug)
        if len(clips) > 1:
            item_debug["clipIndex"] = k
            item_debug["clipCount"] = len(clips)
        items.append({
            "parliament": PARLIAMENT,
            "electoralPeriod": {"number": period},
            "session": {"number": number},
            "agendaItem": deepcopy(agenda),
            "dateStart": start_dt.isoformat("T", "seconds"),
            "dateEnd": end_dt.isoformat("T", "seconds"),
            "media": {
                "videoFileURI": _video_file_uri(url),
                # The feed's own share link (ENLACEDIFERIDO) drops a digit of
                # the intervention id, so build it from the MP4 file name.
                "sourcePage": intervention_page_url(video["leg"], video["iid"]),
                "duration": duration,
                "license": MEDIA_LICENSE,
                "originMediaID": video["stem"],
                "creator": MEDIA_CREATOR,
            },
            "people": [deepcopy(person)] if person.get("label") else [],
            "debug": item_debug,
        })
    return items


def _iid(item: dict) -> int:
    return int(item["media"]["originMediaID"].rsplit("_", 1)[1])


def _insert_later_clip(output: list, item: dict) -> None:
    """Insert a record's 2nd+ clip after the item with the next-lower
    intervention id (the turns recorded between the clips), keeping the
    order of all other items. Its times are clamped to its predecessor's so
    dateStart stays monotonic."""
    lower = [(_iid(o), i) for i, o in enumerate(output) if _iid(o) < _iid(item)]
    pos = max(lower)[1] + 1 if lower else 0
    if pos > 0:
        prev_start = output[pos - 1]["dateStart"]
        if datetime.fromisoformat(item["dateStart"]) < datetime.fromisoformat(prev_start):
            item["dateStart"] = prev_start
            if datetime.fromisoformat(item["dateEnd"]) < datetime.fromisoformat(prev_start):
                item["dateEnd"] = prev_start
            item["media"]["duration"] = (datetime.fromisoformat(item["dateEnd"])
                                         - datetime.fromisoformat(item["dateStart"])).total_seconds()
    output.insert(pos, item)


def parse_media_data(raw: dict) -> dict:
    """Parse a raw per-session interventions file into the media stream dict."""
    meta = raw.get("meta", {})
    sid = meta.get("session")
    period = meta.get("period")
    number = meta.get("sessionNumber")
    records = raw.get("interventions", [])

    output = []
    later_clips = []
    for idx, rec in enumerate(records):
        items = parse_intervention(rec, period, number)
        if not items:
            logger.debug(f"Skipping intervention {idx} in {sid}: no video or no usable date")
            continue
        items[0]["_order"] = idx  # stable tiebreaker for equal HH:MM timestamps
        output.append(items[0])
        later_clips.extend(items[1:])

    output.sort(key=lambda i: (i["dateStart"], i["_order"]))
    for item in sorted(later_clips, key=_iid):
        _insert_later_clip(output, item)
    for i, item in enumerate(output):
        item.pop("_order", None)
        item["speechIndex"] = i + 1

    if not output:
        logger.warning(f"No usable interventions for session {sid}")
        session_start = session_end = None
    else:
        session_start = output[0]["dateStart"]
        session_end = max(i["dateEnd"] for i in output)

    return {
        "meta": {
            "session": sid,
            "processing": {
                "parse_media": datetime.now().isoformat("T", "seconds"),
            },
            "dateStart": session_start,
            "dateEnd": session_end,
        },
        "data": output,
    }


def parse_media_directory(directory: Path):
    """Update parsed media files from raw-*-media.json sources."""
    directory = Path(directory)
    for source in sorted(directory.glob("raw-*-media.json")):
        output_file = source.parent / source.name[4:]  # strip "raw-"
        if not output_file.exists() or output_file.stat().st_mtime < source.stat().st_mtime:
            raw = json.loads(source.read_text())
            data = parse_media_data(raw)
            logger.info(f"Converting {source.name}")
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Congreso raw interventions JSON into the media stream.")
    parser.add_argument("sources", type=str, nargs="*", help="Raw JSON file(s) or a directory")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    if not args.sources:
        parser.print_help()
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    for source in args.sources:
        source = Path(source)
        if source.is_dir():
            parse_media_directory(source)
        else:
            data = parse_media_data(json.loads(source.read_text()))
            json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
