#! /usr/bin/env python3
"""FR Assemblée nationale common config.

Session-key convention: the Syceron compte-rendu uid with the fixed
``CRSANR5L{leg}S`` prefix stripped, e.g. ``CRSANR5L17S2026O1N232`` →
``2026O1N232``. The pieces are:

- ``2026`` — the *session* year. The AN groups séances into an annual
  parliamentary session (``Session ordinaire 2025-2026`` → ``S2026``); the
  séance number restarts each session, so the year is needed for uniqueness.
- ``O1`` / ``E1`` — session ordinaire / extraordinaire (+ ordinal).
- ``N232`` — the séance number within that session.

One Stage 2 session represents one *séance* (sitting); the AN runs several
séances per calendar day (1ère/2ème/3ème séance), each with its own compte
rendu and video, so we key on the séance rather than the day.

``electoralPeriod.number`` is the legislature (17). Because the séance number
resets each session-year, ``session.number`` is encoded so it stays unique
within the legislature — see :func:`session_number_int`.

Clock times: the compte rendu's per-speech ``stime`` counts seconds from the
start of the séance *recording*, which begins before the sitting opens. The
recording's start is the local (Europe/Paris) timestamp in its file name
(``…/hemi_20260529211831.smil/master.m3u8``), see :func:`recording_start`.
"""

import datetime
import json
import re
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401

# Legislature 17 began with the snap election of July 2024; the first session
# year (``Session ordinaire 2024-2025``) is labelled S2025 in the uid.
LEGISLATURE_START_YEAR = 2024

# CRSANR5L17S2026O1N232  →  (17, 2026, "O", 1, 232)
_UID_RE = re.compile(
    r"^CRSANR5L(?P<leg>\d+)S(?P<year>\d{4})(?P<stype>[OE])(?P<ordre>\d+)N(?P<num>\d+)$"
)
# Session key (uid with the leading "CRSANR5L{leg}S" stripped): 2026O1N232
_KEY_RE = re.compile(r"^(?P<year>\d{4})(?P<stype>[OE])(?P<ordre>\d+)N(?P<num>\d+)$")


def uid_to_session(uid: str) -> str:
    """``CRSANR5L17S2026O1N232`` → session key ``2026O1N232``."""
    m = _UID_RE.match(uid)
    if not m:
        raise ValueError(f"unrecognized compte-rendu uid: {uid!r}")
    return f"{m['year']}{m['stype']}{m['ordre']}N{m['num']}"


def session_to_uid(session: str, legislature: int = 17) -> str:
    """``2026O1N232`` → ``CRSANR5L17S2026O1N232`` (inverse of uid_to_session)."""
    m = _KEY_RE.match(session)
    if not m:
        raise ValueError(f"unrecognized session key: {session!r}")
    return f"CRSANR5L{legislature}S{session}"


def parse_session(session: str) -> tuple[int, str, int, int]:
    """Return ``(year, session_type, ordre, num)`` for a session key."""
    m = _KEY_RE.match(session)
    if not m:
        raise ValueError(f"unrecognized session key: {session!r}")
    return int(m["year"]), m["stype"], int(m["ordre"]), int(m["num"])


def session_number_int(session: str) -> int:
    """Encode the session key into a legislature-unique integer.

    ``(year - 2024) * 1000 + type_offset + num`` where ``type_offset`` is 0 for
    a session ordinaire and 500 for a session extraordinaire. The séance number
    restarts each session-year (so ``S2025O1N037`` and ``S2026O1N037`` would
    otherwise collide); multiplying the year offset by 1000 keeps each year's
    séances in their own band. Ordinaire numbers stay well below 500 and
    extraordinaire séances are few, so the two types never overlap within a
    year. Same composite-encoding class as FI / TW — the platform's 4-digit
    session slot still fits for legislature 17.
    """
    year, stype, _ordre, num = parse_session(session)
    type_offset = 0 if stype == "O" else 500
    return (year - LEGISLATURE_START_YEAR) * 1000 + type_offset + num


PARIS = ZoneInfo("Europe/Paris")

# Séance recordings are named by their local start time: hemi_YYYYMMDDHHMMSS.
_RECORDING_RE = re.compile(r"(hemi_(\d{14}))")

CR_PAGE_URL = "https://www.assemblee-nationale.fr/dyn/{leg}/comptes-rendus/seance/{uid}"


def recording_id(hls_url: str) -> Optional[str]:
    """``…/hemi_20260529211831.smil/master.m3u8`` → ``hemi_20260529211831``."""
    m = _RECORDING_RE.search(hls_url or "")
    return m.group(1) if m else None


def recording_start(hls_url: str) -> Optional[datetime.datetime]:
    """Start of the séance recording as an aware Europe/Paris datetime.

    The file-name timestamp is Paris wall-clock time in both CET and CEST
    (a 15:00 sitting in November is recorded from ``hemi_…144538``).
    """
    m = _RECORDING_RE.search(hls_url or "")
    if not m:
        return None
    try:
        dt = datetime.datetime.strptime(m.group(2), "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return dt.replace(tzinfo=PARIS)


def seance_start(raw: Optional[str]) -> Optional[datetime.datetime]:
    """Scheduled séance start from ``<dateSeance>`` (``YYYYMMDDHHMMSSmmm``)."""
    if not raw or len(raw) < 14:
        return None
    try:
        dt = datetime.datetime.strptime(raw[:14], "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return dt.replace(tzinfo=PARIS)


def cr_page_url(session: str, legislature: int = 17) -> str:
    """HTML compte rendu of a séance, addressed by its uid (redirects to the
    human-readable slug URL)."""
    return CR_PAGE_URL.format(leg=legislature, uid=session_to_uid(session, legislature))


class Config(BaseConfig):
    # Sessions are discovered from the raw Syceron compte-rendu files written by
    # the scraper (``{session}-cr.xml``); proceedings are the merge spine.
    SESSION_SOURCE = "proceedings"
    MEDIA_GLOB_SUFFIX = "-cr.xml"

    def raw_cr(self, session: str) -> Path:
        """Raw Syceron compte-rendu XML for one séance."""
        return self.dir('proceedings') / f"{session}-cr.xml"

    def raw_event(self, session: str) -> Path:
        """Resolved video reference (HLS master URL + recording start) for one séance."""
        return self.dir('media') / f"{session}-event.json"

    def status(self, session: str) -> set:
        """Return the status for the given session."""
        status = set()
        if self.file(session, 'media').exists():
            status.add(SessionStatus.media)
        if self.file(session, 'proceedings').exists():
            status.add(SessionStatus.proceedings)
        if self.file(session, 'merged').exists():
            status.add(SessionStatus.merged)
        sfile = self.file(session, 'processed')
        if sfile.exists():
            status.add(SessionStatus.session)
            with open(sfile, 'r') as f:
                data = json.load(f)['data']
            if len(data) == 0:
                status.add(SessionStatus.empty)
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            for s in data:
                tcs = s.get('textContents') or []
                if not tcs or not any((tc.get('textBody') or []) for tc in tcs):
                    status.add(SessionStatus.no_text)
                    continue
                if s.get('debug', {}).get('alignDuration'):
                    status.add(SessionStatus.aligned)
                if s.get('debug', {}).get('nerDuration'):
                    status.add(SessionStatus.ner)
        return status
