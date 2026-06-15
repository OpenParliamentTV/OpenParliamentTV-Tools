#! /usr/bin/env python3

# Convert the per-session raw interventions file (written by
# scraper/fetch_interventions.py) into the intermediate "media" JSON stream:
# one record per speech, carrying the per-speech video, speaker, agenda and
# (coarse, HH:MM) timing. The proceedings stream supplies the transcript text;
# the merger joins the two by session + speaker surname.

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

try:
    from parsers.common import parse_orador
except ModuleNotFoundError:
    base_dir = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(base_dir))
    from parsers.common import parse_orador

_repo_root = str(Path(__file__).resolve().parents[4])
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from optv.shared.agenda_types import annotate_agenda_item, classify_es
from optv.parliaments import get_rights as _get_rights

MADRID = ZoneInfo("Europe/Madrid")
PARLIAMENT = "ES"
# Single Aviso Legal covers both video and proceedings reuse.
MEDIA_LICENSE = _get_rights("ES", stream="media")["license"]
MEDIA_CREATOR = _get_rights("ES", stream="media")["creator"]


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


def _video_id(mp4: str) -> str:
    """Return the video id (filename stem) of a real per-speech MP4, else ''.

    Some interventions carry a placeholder URL with an empty id
    (".../video/leg/400//cortes/.mp4") — i.e. no video. Those return ''.
    """
    if not mp4:
        return ""
    filename = os.path.basename(mp4)
    stem = filename[:-4] if filename.lower().endswith(".mp4") else os.path.splitext(filename)[0]
    return stem.strip()


def parse_intervention(rec: dict, period: int, number: int) -> dict:
    """Convert one raw intervention record into a media-stream item.

    Returns {} for records the media stream cannot represent (no real video,
    or no usable date).
    """
    mp4 = rec.get("ENLACEDESCARGADIRECTA", "")
    media_id = _video_id(mp4)
    if not media_id:
        # No per-speech video — not a media-stream item. The proceedings
        # stream still carries this speaker's text into the merge.
        return {}

    sesion = rec.get("SESION", "")
    start = _parse_dt(sesion, rec.get("INICIOINTERVENCION", ""))
    end = _parse_dt(sesion, rec.get("FININTERVENCION", ""))

    debug: dict = {}
    if start is None:
        # No usable date — fall back to nothing; caller will drop.
        return {}
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

    objeto = (rec.get("OBJETOINICIATIVA") or "").strip()
    fase = (rec.get("FASE") or "").strip()
    title = objeto or fase or "Sesión plenaria"
    agenda = {
        "title": title,
        "officialTitle": objeto or fase or title,
    }
    nt, ct = classify_es(objeto, fase, rec.get("TIPOINTERVENCION"))
    annotate_agenda_item(agenda, nt, ct)

    person = parse_orador(rec.get("ORADOR", ""), rec.get("CARGOORADOR", ""))

    item: dict = {
        "parliament": PARLIAMENT,
        "electoralPeriod": {"number": period},
        "session": {"number": number},
        "agendaItem": agenda,
        "dateStart": start_dt.isoformat("T", "seconds"),
        "dateEnd": end_dt.isoformat("T", "seconds"),
        "media": {
            "videoFileURI": mp4,
            "sourcePage": rec.get("ENLACEDIFERIDO", ""),
            "duration": duration,
            "license": MEDIA_LICENSE,
            "originMediaID": media_id,
            "creator": MEDIA_CREATOR,
        },
        "people": [person] if person.get("label") else [],
        "debug": debug,
    }
    return item


def parse_media_data(raw: dict) -> dict:
    """Parse a raw per-session interventions file into the media stream dict."""
    meta = raw.get("meta", {})
    sid = meta.get("session")
    period = meta.get("period")
    number = meta.get("sessionNumber")
    records = raw.get("interventions", [])

    output = []
    for idx, rec in enumerate(records):
        item = parse_intervention(rec, period, number)
        if not item:
            logger.debug(f"Skipping intervention {idx} in {sid}: no usable date")
            continue
        item["_order"] = idx  # stable tiebreaker for equal HH:MM timestamps
        output.append(item)

    output.sort(key=lambda i: (i["dateStart"], i["_order"]))
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
