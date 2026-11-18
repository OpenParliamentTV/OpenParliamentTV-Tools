#! /usr/bin/env python3
"""Build a session media JSON from the verkkolähetys ``{session}-event.json``.

Eduskunta publishes one HLS stream per plenary session; per-speech navigation
uses ``time`` / ``endTime`` (seconds into the session video) from the broadcast
``speakers[]`` array. Each speaker entry also carries ``personNumber`` (the
relational join key to the speech records), ``topicId`` (agenda item),
``onkoVastauspuheenvuoro`` (reply flag) and an absolute UTC ``timeStamp``.

Output: ``original/media/{session}-media.json`` — one record per speaker with a
Stage-2-shaped ``media`` block. ``videoFileURI`` is the HLS master + a
``#t=start,end`` media fragment so each speech is uniquely addressable;
``audioFileURI`` is the clean HLS master (no fragment) used as the slicing
source in ``align_prep.py``; ``startOffset`` / ``duration`` drive that slicing.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Any, Optional

from optv.parliaments.FI.common import Config, parse_session_str
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

VL_ROOT = "https://verkkolahetys.eduskunta.fi"
SOURCE_CREATOR = _get_rights("FI", stream="media")["creator"]
SOURCE_LICENSE = _get_rights("FI", stream="media")["license"]


def _int(v) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _seconds(v) -> Optional[float]:
    """A broadcast offset in seconds, or ``None`` when the event has none (absent, or the string ``"null"`` the page writes for a missing ``endTime``)."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# A wrong pick that the operator corrects back lasts a few seconds, and the
# person picked first is entered again shortly after the segment's start.
WRONG_PICK_MAX_S = 30.0
REPICK_WITHIN_S = 15.0


def drop_replaced_picks(speakers: list[dict]) -> list[dict]:
    """The speaker entries without the picks the broadcast operator replaced.

    The operator marks who has the floor. Replacing a pick creates a new entry that inherits the start of the running segment and leaves the replaced entry without an end, so an end-less entry followed by an entry with the same start is a replaced pick (several in a row form a chain). Two cases:

    - The first pick was wrong and its successor carries the speech: only the replaced entries go.
    - The first pick was right, the operator switched to someone else for a few seconds and then entered the first person again: the short successor is a wrong pick too and goes as well, leaving the re-entered speech.
    """
    kept: list[dict] = []
    i, n = 0, len(speakers)
    while i < n:
        start = _seconds(speakers[i].get("time"))
        replaced: list[dict] = []
        while (start is not None and i + 1 < n
               and _seconds(speakers[i].get("endTime")) is None
               and _seconds(speakers[i + 1].get("time")) == start):
            replaced.append(speakers[i])
            i += 1
        entry = speakers[i]
        i += 1
        if replaced and i < n:
            end = _seconds(entry.get("endTime"))
            again = speakers[i]
            again_start = _seconds(again.get("time"))
            again_person = _int(again.get("personNumber"))
            if (end is not None and end - start <= WRONG_PICK_MAX_S
                    and again_start is not None and 0 <= again_start - start <= REPICK_WITHIN_S
                    and again_person is not None
                    and again_person != _int(entry.get("personNumber"))
                    and again_person in {_int(r.get("personNumber")) for r in replaced}):
                continue
        kept.append(entry)
    return kept


def speaker_media_record(spk: dict, event: dict, session_url: str,
                         next_start: Optional[float] = None) -> Optional[dict]:
    """The media record of one speaker entry of the broadcast event.

    ``next_start`` is the start offset of the entry that follows. An entry without an end of its own ends there, when that is later than its own start; otherwise it has no duration.
    """
    person_number = _int(spk.get("personNumber"))
    start = _seconds(spk.get("time"))
    end = _seconds(spk.get("endTime"))
    if start is None:
        return None
    if end is None and next_start is not None and next_start > start:
        end = next_start
    duration = max(0.0, (end - start)) if end is not None else 0.0

    hls = event.get("hlsUrl") or ""
    event_id = event.get("eventId") or ""
    video_uri = f"{hls}#t={start:.0f},{(end if end is not None else start):.0f}" if hls else ""

    # One HLS stream serves the whole session, so the bare session URL is
    # identical for every speech. The platform keys speech identity on
    # sourcePage, so append the per-speech start offset (seconds) to keep each
    # distinct — ``?start=`` is also the player's seek position.
    source_page = f"{session_url}?start={start:.0f}"

    media: dict[str, Any] = {
        "videoFileURI": video_uri,
        "audioFileURI": hls,                 # session master; align_prep slices by offset
        "sourcePage": source_page,
        "creator": SOURCE_CREATOR,
        "license": SOURCE_LICENSE,
        "aligned": False,
        "duration": duration,
        "videoStreamURI": hls or None,
        "videoArchiveURI": None,
        "additionalInformation": {
            "startOffset": start,
            "eventRef": event_id,
            "timeStamp": spk.get("timeStamp"),
        },
    }
    return {
        "personNumber": person_number,
        "timeStamp": spk.get("timeStamp"),
        "topicId": str(spk.get("topicId") or ""),
        "isReply": bool(spk.get("onkoVastauspuheenvuoro")),
        "party": spk.get("party") or {},
        "firstName": spk.get("firstName") or "",
        "lastName": spk.get("lastName") or "",
        "media": media,
    }


def parse_media_for_session(config: Config, session: str) -> dict:
    event_path = config.raw_event(session)
    if not event_path.exists():
        sys.exit(f"Event file not found: {event_path}")
    event = json.loads(event_path.read_text())
    year, number = parse_session_str(session)
    session_url = f"{VL_ROOT}/fi/taysistunnot/taysistunto-{number}-{year}"

    listed = event.get("speakers") or []
    speakers = drop_replaced_picks(listed)
    if len(speakers) != len(listed):
        logger.info(f"[{session}] dropped {len(listed) - len(speakers)} replaced speaker pick(s)")
    records: list[dict] = []
    for index, spk in enumerate(speakers):
        next_start = _seconds(speakers[index + 1].get("time")) if index + 1 < len(speakers) else None
        rec = speaker_media_record(spk, event, session_url, next_start=next_start)
        if rec is not None:
            records.append(rec)

    return {
        "meta": {
            "session": session,
            "sessionStarted": event.get("sessionStarted"),
            "sessionEnded": event.get("sessionEnded"),
            "processing": {
                "parse_media": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": records,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 2026-058")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = parse_media_for_session(config, args.session)
    out = config.file(args.session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} media records)")


if __name__ == "__main__":
    main()
