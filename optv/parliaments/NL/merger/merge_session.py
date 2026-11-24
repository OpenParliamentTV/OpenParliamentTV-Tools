"""Merge the proceedings and the Debat Direct records of one NL sitting into Stage 2.

The report is the spine: every speech, its text and its start and end clock times come from it. Debat Direct adds the video. A speech's agenda item carries the id of its Debat Direct debate, whose record names the day's recording of the plenary hall; the speech is addressed as a time range of that recording, which the stream server cuts out, and linked to the moment Debat Direct marks at its start.

Nothing is matched by content, so no speech can receive another speech's text. A speech of an agenda item that Debat Direct does not list (usually the opening) is cut from the same day's recording and links to the report page instead.

Run for one session: ``python -m optv.parliaments.NL.merger.merge_session <session> <data_dir>``.
"""

import datetime
import json
import logging
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from optv.parliaments import get_language, get_rights
from optv.shared.cli import session_cli
from optv.shared.meta import build_meta, fill_original_language, now_iso
from optv.shared.publish import save_if_changed
from optv.shared.speech_id import normalize_speech_originid

from ..common import period_of, session_number

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

# Clock times as Debat Direct writes them, in the stream's range parameters and
# in the ``event`` parameter of its pages.
_CLOCK = "%Y-%m-%dT%H:%M:%S%z"

# The moments of a debate a speech can start at, by who has the floor.
_SPEAKER_MOMENTS = ("speaker", "chairman", "interrupter")


def _clock(moment: datetime.datetime) -> str:
    return quote(moment.strftime(_CLOCK), safe="")


def clip_url(stream: str, start: datetime.datetime, end: datetime.datetime) -> str:
    """The recording cut to ``start``–``end``. The server answers with whole segments, so a clip begins up to one segment before ``start`` and ends one to two segments after ``end``."""
    return f"{stream}?start={_clock(start)}&end={_clock(end)}"


def _starting_moment(debate: dict, start: datetime.datetime, end: datetime.datetime) -> Optional[dict]:
    """The Debat Direct moment a speech is linked to: a speaker moment at its start, else the first one inside it, else whatever is marked at its start."""
    events = [(datetime.datetime.fromisoformat(e["time"]), e) for e in debate.get("events") or []]
    at_start = [e for time, e in events if time == start]
    inside = [e for time, e in events if start <= time < end and e["type"] in _SPEAKER_MOMENTS]
    for candidates in ([e for e in at_start if e["type"] in _SPEAKER_MOMENTS], inside, at_start):
        if candidates:
            return candidates[0]
    return None


def source_page(debate: Optional[dict], speech: dict, report_page: str,
                start: datetime.datetime, end: datetime.datetime) -> str:
    """The page a speech is identified by: its moment on the debate's Debat Direct page.

    Where Debat Direct marks no moment in the speech, the link names the speech's start time; where it has no debate, the link is the report page with the speech's id as fragment. Both keep the address unique per speech.
    """
    if debate is None or not debate.get("sourcePage"):
        return f"{report_page}#{speech['originTextID']}"
    moment = _starting_moment(debate, start, end)
    if moment is not None:
        kind, time = moment["type"], datetime.datetime.fromisoformat(moment["time"])
    else:
        kind = "chairman" if speech["people"][0]["context"] == "president" else "speaker"
        time = start
    return f"{debate['sourcePage']}?event={kind}{_clock(time)}"


def _stream_for(speech: dict, debates: dict[str, dict], start: datetime.datetime) -> tuple[Optional[dict], Optional[str]]:
    """The speech's debate and the recording to cut it from: the debate's, else the one of another debate that day."""
    debate = debates.get(speech["activityId"])
    if debate is not None and debate.get("stream"):
        return debate, debate["stream"]
    day = start.date().isoformat()
    for other in debates.values():
        if other.get("stream") and other.get("date") == day:
            return debate, other["stream"]
    return debate, None


def speech_record(speech: dict, debates: dict[str, dict], *, session: str, period: int,
                  session_dates: tuple[Optional[str], Optional[str]], report_page: str) -> Optional[dict]:
    """One Stage 2 speech, or ``None`` when no recording covers it."""
    start = datetime.datetime.fromisoformat(speech["dateStart"])
    end = datetime.datetime.fromisoformat(speech["dateEnd"])
    debate, stream = _stream_for(speech, debates, start)
    if stream is None:
        return None

    media_rights = get_rights(PARLIAMENT_ID, period, "media")
    text_rights = get_rights(PARLIAMENT_ID, period, "proceedings")
    media = {
        "videoFileURI": clip_url(stream, start, end),
        "sourcePage": source_page(debate, speech, report_page, start, end),
        # The speech's own length. The clip is one to three segments longer; the
        # align stage writes the clip's measured length here (align_prep.py).
        "duration": (end - start).total_seconds(),
        "aligned": False,
        "creator": media_rights.get("creator", ""),
        "license": media_rights.get("license", ""),
        "additionalInformation": {"stream": stream},
    }
    if debate is not None:
        media["originMediaID"] = debate["id"]

    debug = {"turns": speech["debug"]["turns"], "kind": speech["debug"]["kind"]}
    if speech["debug"].get("part"):
        debug["part"] = speech["debug"]["part"]
    text_contents = []
    if speech.get("textBody"):
        text_contents.append({
            "type": "proceedings",
            "language": get_language(PARLIAMENT_ID),
            "originTextID": speech["originTextID"],
            "sourceURI": report_page,
            "creator": text_rights.get("creator", ""),
            "license": text_rights.get("license", ""),
            "textBody": speech["textBody"],
        })
        debug["proceedingIndex"] = speech["speechIndex"]

    session_info = {"number": session_number(period, session)}
    if session_dates[0]:
        session_info["dateStart"] = session_dates[0]
    if session_dates[1]:
        session_info["dateEnd"] = session_dates[1]
    return {
        "parliament": PARLIAMENT_ID,
        "electoralPeriod": {"number": period},
        "session": session_info,
        "agendaItem": speech["agendaItem"],
        "dateStart": speech["dateStart"],
        "dateEnd": speech["dateEnd"],
        "people": speech["people"],
        "textContents": text_contents,
        "media": media,
        "documents": [],
        "debug": debug,
    }


def merge_documents(proceedings: dict, media: dict, session: str) -> dict:
    """The Stage 2 document of a sitting from its parsed proceedings and media documents."""
    period = period_of(session)
    if period is None:
        raise ValueError(f"[{session}] the sitting lies before the first period this parliament is set up for")
    meta = proceedings["meta"]
    debates = {d["id"]: d for d in media.get("data") or []}

    speeches = []
    dropped = 0
    for speech in proceedings.get("data") or []:
        record = speech_record(speech, debates, session=session, period=period,
                               session_dates=(meta.get("dateStart"), meta.get("dateEnd")),
                               report_page=meta["sourceURI"])
        if record is None:
            dropped += 1
            continue
        record["speechIndex"] = len(speeches) + 1
        normalize_speech_originid(record)
        speeches.append(record)
    fill_original_language(speeches, PARLIAMENT_ID)

    linked = sum(1 for s in speeches if "?event=" in s["media"]["sourcePage"])
    logger.info(f"[{session}] merged {len(speeches)} speeches, {linked} linked to a Debat Direct page"
                + (f", {dropped} dropped for lack of a recording" if dropped else ""))
    return {
        "meta": build_meta(
            PARLIAMENT_ID,
            session=session,
            electoral_period=period,
            date_start=meta.get("dateStart"),
            date_end=meta.get("dateEnd"),
            processing={
                **(media.get("meta") or {}).get("processing", {}),
                **meta.get("processing", {}),
                "merge": now_iso(),
            },
            extra={
                "sourceLabel": meta.get("sourceLabel"),
                "sourceURI": meta.get("sourceURI"),
                "reportStatus": (meta.get("report") or {}).get("status"),
            },
        ),
        "data": speeches,
    }


def merge_session(session: str, config, options=None) -> Path:
    """Write ``cache/merged/{session}-merged.json`` and return its path. An unchanged merge leaves the file untouched."""
    proceedings_file = config.file(session, "proceedings")
    media_file = config.file(session, "media")
    for path in (proceedings_file, media_file):
        if not path.exists():
            raise FileNotFoundError(f"[{session}] parsed file missing: {path}")
    doc = merge_documents(json.loads(proceedings_file.read_text(encoding="utf-8")),
                          json.loads(media_file.read_text(encoding="utf-8")), session)
    out = config.file(session, "merged", create=True)
    if save_if_changed(doc, out):
        logger.info(f"[{session}] wrote {out.name}")
    else:
        logger.info(f"[{session}] no change, {out.name} left as it is")
    return out


if __name__ == "__main__":
    session_cli(PARLIAMENT_ID, merge_session, description=__doc__, example="2026-005")
