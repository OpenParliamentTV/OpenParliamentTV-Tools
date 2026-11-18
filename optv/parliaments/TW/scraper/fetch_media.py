#! /usr/bin/env python3
"""Fetch the IVOD index for Legislative Yuan plenary meetings.

Which meetings: the codes passed via ``args.tw_meeting_code`` (``--session``,
repeatable), or, when none are given, every plenary meeting with IVODs on a
day from ``--since`` (default: the last 14 days) to today, discovered via
``ly.govapi.tw/v2/ivods?日期=YYYY-MM-DD``. For each meeting we write one
``original/media/{session}-ivods.json`` containing every IVOD record returned
by ``ly.govapi.tw/v2/ivods?會議資料.會議代碼=<code>``. The detail (incl.
transcript) for each IVOD is fetched by :mod:`.fetch_proceedings`.

A plenary meeting code can span several sitting days (e.g. ``院會-11-6-5`` sat
on 2026-10-30 and 2026-11-03), so a cached index is re-fetched when discovery
finds a sitting day it does not contain yet.

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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from optv.parliaments.TW.common import Config, encode_session
from .ly_api import LYApiClient

logger = logging.getLogger(__name__)

PLENARY_RE = re.compile(r"^院會-(\d+)-(\d+)-(\d+)$")

# Discovery window when neither --session nor --since is given.
DEFAULT_LOOKBACK_DAYS = 14

# Sitting dates are Taiwan-local.
TAIPEI = timezone(timedelta(hours=8))


def today_taipei() -> date:
    return datetime.now(TAIPEI).date()


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
    expected_dates: set[str] | None = None,
) -> Path | None:
    """Download the IVOD list for one plenary meeting.

    A cached index is reused unless ``force`` or it lacks one of
    ``expected_dates`` (sitting days found by discovery). The file is only
    rewritten when the listing changed, so its mtime keeps driving the parse.

    Returns the path to the IVOD index, or ``None`` if the meeting has no
    IVODs yet (e.g. the day hasn't been published).
    """
    try:
        term, sp, mn = parse_meeting_code(meeting_code)
    except ValueError as e:
        logger.error("Skipping %s: %s", meeting_code, e)
        return None

    session = encode_session(term, sp, mn)
    out = config.file(session, "ivods", create=True)
    cached_ivods = None
    if out.exists() and not force:
        cached_ivods = json.loads(out.read_text()).get("ivods") or []
        have = {iv.get("日期") for iv in cached_ivods}
        missing = sorted(set(expected_dates or ()) - have)
        if not missing:
            logger.info(f"[{session}] media index cached → {out.name}")
            return out
        logger.info(f"[{session}] new sitting day(s) {missing}; refreshing IVOD list")

    logger.info(f"[{session}] fetching IVOD list for {meeting_code}")
    ivods = client.list_ivods_for_meeting(meeting_code)
    if not ivods:
        logger.warning(f"[{session}] no IVODs returned for {meeting_code}; "
                       "the meeting may not be published yet.")
        return out if out.exists() else None
    if cached_ivods is not None and ivods == cached_ivods:
        logger.info(f"[{session}] IVOD list unchanged → {out.name}")
        return out

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


def discover_plenary_meetings(client: LYApiClient, since: date, until: date,
                              *, term: int | None = None) -> dict[str, set[str]]:
    """Plenary meetings with IVODs on any day in ``[since, until]``.

    Returns ``{meeting_code: {sitting dates}}``. ``term`` (if given) keeps only
    that term's meetings. Codes outside the ``院會-T-SP-MN`` shape (e.g.
    extraordinary sessions) are skipped.
    """
    found: dict[str, set[str]] = {}
    day = since
    while day <= until:
        for code, dates in client.list_plenary_codes_for_date(day.isoformat()).items():
            try:
                code_term, _, _ = parse_meeting_code(code)
            except ValueError:
                logger.info("Skipping non-standard plenary code %s", code)
                continue
            if term is not None and code_term != term:
                continue
            found.setdefault(code, set()).update(dates)
        day += timedelta(days=1)
    return found


def download_media(config: Config, args) -> list[str]:
    """Workflow hook: fetch the IVOD list for every requested or discovered
    plenary meeting.

    Returns the list of resolved session keys.
    """
    client = LYApiClient(
        cache_dir=config.dir("cache", create=True) / "ly-api",
        retry_count=getattr(args, "retry_count", 5) or 5,
        retry_delay_max=getattr(args, "retry_delay_max", 10.0),
    )
    codes = list(getattr(args, "tw_meeting_code", []) or [])
    if codes:
        targets: dict[str, set[str]] = {code: set() for code in codes}
    else:
        until = today_taipei()
        since = getattr(args, "since", None) or (until - timedelta(days=DEFAULT_LOOKBACK_DAYS))
        term = args.period if getattr(args, "limit_to_period", True) else None
        logger.info(f"Discovering plenary meetings from {since} to {until}")
        targets = discover_plenary_meetings(client, since, until, term=term)
        logger.info(f"Discovered {len(targets)} plenary meeting(s): {sorted(targets)}")

    sessions: list[str] = []
    for code in sorted(targets):
        path = fetch_meeting_index(code, config, client, force=args.force,
                                   expected_dates=targets[code])
        if path is not None:
            sessions.append(path.name.split("-ivods.json")[0])
    return sessions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--meeting-code", action="append", default=[],
                        help="Plenary meeting code, e.g. 院會-11-5-11 (repeatable). "
                             "Without it, meetings are discovered by date.")
    parser.add_argument("--since", type=date.fromisoformat, default=None,
                        help="Discover meetings from this date (YYYY-MM-DD) to today")
    parser.add_argument("--period", type=int, default=11)
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
