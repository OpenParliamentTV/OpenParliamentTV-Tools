"""Turn the Debat Direct records of an NL sitting into ``original/media/{session}-media.json``.

One record per debate (an agenda item of the report, by the same id): the day's recording of the plenary hall, the debate's page on Debat Direct, and the moments Debat Direct marks in it (a speaker starting, the chair speaking, an interruption). The merger addresses each speech as a time range of the recording and links it to the moment it starts at.

Run on its own: ``python -m optv.parliaments.NL.parsers.media2json <data_dir> --session 2026-005``.
"""

import argparse
import datetime
import json
import logging
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from optv.shared.cli import load_config, setup_logging
from optv.shared.meta import now_iso

from ..common import in_scope

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

DEBATDIRECT_SITE = "https://debatdirect.tweedekamer.nl"
# The category a debate without one is filed under on the site.
DEFAULT_CATEGORY = "overig"


def _moment(value: Optional[str]) -> Optional[str]:
    """Debat Direct's ``2026-09-23T11:41:39+0200`` as ISO 8601 with a colon in the offset."""
    if not value:
        return None
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z").isoformat(timespec="seconds")
    except ValueError:
        return None


def _stream(debate: dict) -> Optional[str]:
    """The recording's HLS address without the player's query parameters."""
    url = (debate.get("video") or {}).get("vodUrl")
    if not url:
        return None
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def debate_page(debate: dict) -> Optional[str]:
    """The debate's page on the Debat Direct site: date, first category, room, slug."""
    date, room, slug = debate.get("debateDate"), debate.get("locationId"), debate.get("slug")
    if not (date and room and slug):
        return None
    category = (debate.get("categoryIds") or [DEFAULT_CATEGORY])[0]
    return f"{DEBATDIRECT_SITE}/{date}/{category}/{room}/{slug}"


def debate_record(debate: dict) -> dict:
    events = []
    for event in debate.get("events") or []:
        time = _moment(event.get("eventStart"))
        if time and event.get("eventType"):
            events.append({"time": time, "type": event["eventType"], "objectId": event.get("objectId")})
    events.sort(key=lambda e: datetime.datetime.fromisoformat(e["time"]))
    return {
        "id": debate["id"],
        "name": debate.get("name"),
        "debateType": debate.get("debateType"),
        "date": debate.get("debateDate"),
        "dateStart": _moment(debate.get("startedAt") or debate.get("startsAt")),
        "dateEnd": _moment(debate.get("endedAt") or debate.get("endsAt")),
        "stream": _stream(debate),
        "sourcePage": debate_page(debate),
        "events": events,
    }


def parse_debates(raw: dict, session: str) -> dict:
    """The media document of one sitting from the scraper's Debat Direct file."""
    debates = sorted((debate_record(d) for d in (raw.get("debates") or {}).values()),
                     key=lambda d: d["dateStart"] or "")
    without_stream = [d["id"] for d in debates if not d["stream"]]
    if without_stream:
        logger.warning(f"[{session}] {len(without_stream)} debate(s) without a recording: {without_stream}")
    return {
        "meta": {"session": session, "processing": {"parse_media": now_iso()}},
        "data": debates,
    }


def parse_session(config, session: str) -> Path:
    raw = json.loads(config.raw_debates(session).read_text(encoding="utf-8"))
    doc = parse_debates(raw, session)
    out = config.file(session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"[{session}] {len(doc['data'])} debates")
    return out


def parse_media(config, args) -> None:
    """Parse the Debat Direct file of every session in scope whose parsed file is missing or older."""
    for session in config.sessions():
        raw = config.raw_debates(session)
        out = config.file(session, "media")
        if not in_scope(args, session) or not raw.exists():
            continue
        if out.exists() and out.stat().st_mtime >= raw.stat().st_mtime and not getattr(args, "force", False):
            continue
        parse_session(config, session)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", action="append", default=[], help="session key, e.g. 2026-005 (repeatable); default: all")
    parser.add_argument("--force", action="store_true", help="parse again what is already parsed")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    args.limit_to_period = False
    args.limit_session = "^(" + "|".join(re.escape(s) for s in args.session) + ")$" if args.session else ""
    parse_media(load_config(PARLIAMENT_ID, args.data_dir), args)


if __name__ == "__main__":
    main()
