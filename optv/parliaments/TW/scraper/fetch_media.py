#! /usr/bin/env python3
"""Fetch the IVOD index for one Legislative Yuan plenary meeting.

For each meeting code passed via ``args.tw_meeting_code`` (repeatable) we
write one ``original/media/{session}-media.json`` containing every IVOD
record returned by ``ly.govapi.tw/v2/ivods?會議資料.會議代碼=<code>``. The
detail (incl. transcript) for each IVOD is fetched by
:mod:`.fetch_proceedings`.

Session-key derivation: the meeting code parses to ``(term, session_period,
meeting_number)`` → :func:`optv.parliaments.TW.common.encode_session`. So
``院會-11-5-11`` produces session key ``"1105011"``.

The output shape is::

    {
      "meta": {
        "meetingCode": "院會-11-5-11",
        "session": "1105011",
        "fetchedAt": "...",
        "term": 11, "sessionPeriod": 5, "meetingNumber": 11
      },
      "ivods": [<raw IVOD stub, IVOD list endpoint shape>, ...]
    }
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                          # TW/
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))     # repo root
    __package__ = "optv.parliaments.TW.scraper"

from optv.parliaments.TW.common import Config, encode_session
from .ly_api import LYApiClient

logger = logging.getLogger(__name__)

PLENARY_RE = re.compile(r"^院會-(\d+)-(\d+)-(\d+)$")


def parse_meeting_code(code: str) -> tuple[int, int, int]:
    """``院會-11-5-11`` → ``(11, 5, 11)``. Raises ValueError for non-plenary."""
    m = PLENARY_RE.match(code)
    if not m:
        raise ValueError(
            f"Not a plenary meeting code: {code!r}. "
            f"Expected '院會-{{term}}-{{sessionPeriod}}-{{meetingNumber}}'."
        )
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def fetch_meeting_index(
    meeting_code: str,
    config: Config,
    client: LYApiClient,
    *,
    force: bool = False,
) -> Path | None:
    """Download the IVOD list for one plenary meeting.

    Returns the path to the written media JSON, or ``None`` if the meeting
    has no IVODs yet (e.g. the day hasn't been published).
    """
    try:
        term, sp, mn = parse_meeting_code(meeting_code)
    except ValueError as e:
        logger.error("Skipping %s: %s", meeting_code, e)
        return None

    session = encode_session(term, sp, mn)
    out = config.file(session, "ivods", create=True)
    if out.exists() and not force:
        logger.info(f"[{session}] media index cached → {out.name}")
        return out

    logger.info(f"[{session}] fetching IVOD list for {meeting_code}")
    ivods = client.list_ivods_for_meeting(meeting_code)
    if not ivods:
        logger.warning(f"[{session}] no IVODs returned for {meeting_code}; "
                       "the meeting may not be published yet.")
        return None

    doc = {
        "meta": {
            "meetingCode": meeting_code,
            "session": session,
            "term": term,
            "sessionPeriod": sp,
            "meetingNumber": mn,
            "fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "count": len(ivods),
        },
        "ivods": ivods,
    }
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {out.name} ({len(ivods)} IVODs)")
    return out


def download_media(config: Config, args) -> list[str]:
    """Workflow hook: fetch the IVOD list for every ``--tw-meeting-code``.

    Returns the list of resolved session keys.
    """
    codes = list(getattr(args, "tw_meeting_code", []) or [])
    if not codes:
        logger.info("No --tw-meeting-code passed; nothing to download (use "
                    "--tw-meeting-code 院會-11-5-11 [...] to download a plenary).")
        return []

    client = LYApiClient(
        cache_dir=config.dir("cache", create=True) / "ly-api",
        retry_count=getattr(args, "retry_count", 5) or 5,
        retry_delay_max=getattr(args, "retry_delay_max", 10.0),
    )
    sessions: list[str] = []
    for code in codes:
        path = fetch_meeting_index(code, config, client, force=args.force)
        if path is not None:
            sessions.append(path.name.split("-ivods.json")[0])
    return sessions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--meeting-code", action="append", default=[],
                        help="Plenary meeting code, e.g. 院會-11-5-11 (repeatable)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args.tw_meeting_code = args.meeting_code
    args.retry_count = 5
    args.retry_delay_max = 10.0
    config = Config(args.data_dir)
    download_media(config, args)


if __name__ == "__main__":
    main()
