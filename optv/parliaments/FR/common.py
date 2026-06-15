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
"""

import logging
logger = logging.getLogger(__name__)

import json
import re
from pathlib import Path

# Re-exported for back-compat with shared callers.
from optv.shared.publish import (  # noqa: F401
    data_signature,
    save_if_changed,
    data_has_timing,
    data_has_ner,
    is_demotion,
    carry_forward_wids,
    carry_forward_enrichments,
)
from optv.shared.config import BaseConfig
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


class Config(BaseConfig):
    def __init__(self, data_dir: Path, cache_dir: Path = None):
        data_dir = Path(data_dir)
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
        else:
            cache_dir = data_dir / "cache"
        self._dir = {
            'data': data_dir,
            'cache': cache_dir,
            'media': data_dir / "original" / "media",
            'proceedings': data_dir / "original" / "proceedings",
            'merged': cache_dir / "merged",
            'aligned': cache_dir / "aligned",
            'ner': cache_dir / "ner",
            'audio': cache_dir / "audio",
            'audio_session': cache_dir / "audio_session",
            'processed': data_dir / "processed",
            'nel_data': data_dir / "metadata",
        }

    def file(self, session: str, stage: str = 'processed', create=False) -> Path:
        suffix = stage
        d = self._dir[stage]
        if stage == 'processed':
            suffix = 'session'
        if create and not d.is_dir():
            d.mkdir(parents=True)
        return d / f"{session}-{suffix}.json"

    def raw_cr(self, session: str) -> Path:
        """Raw Syceron compte-rendu XML for one séance."""
        return self.dir('proceedings') / f"{session}-cr.xml"

    def raw_event(self, session: str) -> Path:
        """Resolved video reference (CRV id + HLS master URL) for one séance."""
        return self.dir('media') / f"{session}-event.json"

    def data(self, session: str, stage: str = 'processed') -> list:
        filename = self.file(session, stage)
        if filename.exists():
            with open(filename) as f:
                data = json.load(f)
        else:
            logger.warning(f"No data for {session}-{stage}")
            data = []
        return data

    def sessions(self, prefix: str = ''):
        """Return the list of currently existing sessions.

        Built from the raw Syceron compte-rendu files written by the scraper
        (``{session}-cr.xml``); proceedings are the merge spine.
        """
        suffix = '-cr.xml'
        return list(sorted(
            f.name[:-len(suffix)]
            for f in self.dir('proceedings').glob(f'{prefix}*{suffix}')
        ))

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


if __name__ == '__main__':
    import sys
    config = Config(Path(sys.argv[1]))
    import IPython
    IPython.embed()
