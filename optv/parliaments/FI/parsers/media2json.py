#! /usr/bin/env python3
"""Build a session media JSON from the verkkolähetys ``{session}-event.json``.

Eduskunta publishes one HLS stream per plenary session; per-speech navigation
uses ``time`` / ``endTime`` (seconds into the session video) from the broadcast
``speakers[]`` array. Each speaker entry also carries ``personNumber`` (the
relational join key to the PTK proceedings), ``topicId`` (agenda item),
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

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.FI.parsers"

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


def speaker_media_record(spk: dict, event: dict, session_url: str) -> Optional[dict]:
    person_number = _int(spk.get("personNumber"))
    start = spk.get("time")
    end = spk.get("endTime")
    start = float(start) if start is not None else None
    end = float(end) if end is not None else None
    if start is None:
        return None
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

    records: list[dict] = []
    for spk in event.get("speakers") or []:
        rec = speaker_media_record(spk, event, session_url)
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
