#! /usr/bin/env python3
"""NL Tweede Kamer: file layout, session keys and periods.

A session is one plenary sitting (``Vergadering``). The House numbers its sittings within a session year (*vergaderjaar*, September to September), so the session key is ``{year}-{number:03d}``: the session year's first calendar year and the sitting's number in it, e.g. ``2026-005`` for the 5th sitting of 2026-2027.

The House does not number its terms. ``--period`` is the year a House was elected and first met (``2025`` for the House elected on 2025-10-29). A session year can belong to two terms, because an election falls inside it, so the term of a session is read from ``TERM_FIRST_SESSION`` and not from the key's year.

Stage 2 has one integer ``session.number`` per period. It is ``(year - period) * 1000 + number``, which keeps the sittings of a term's session years apart; the sitting as the House cites it is in ``meta.sourceLabel``.
"""

import re
from pathlib import Path
from typing import Optional

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401

# The first sitting of each term, by period. A term runs until the next
# term's first sitting; add an entry when a newly elected House first meets.
TERM_FIRST_SESSION = {
    2025: "2025-012",   # 2025-11-12, the House elected on 2025-10-29
}

_SESSION_RE = re.compile(r"^(\d{4})-(\d{3})$")


def session_key(year: int, number: int) -> str:
    """``(2026, 5)`` → ``"2026-005"``."""
    return f"{int(year)}-{int(number):03d}"


def parse_session_key(session: str) -> tuple[int, int]:
    """``"2026-005"`` → ``(2026, 5)``."""
    m = _SESSION_RE.match(session)
    if not m:
        raise ValueError(f"Not an NL session key: {session!r} (expected YYYY-NNN)")
    return int(m.group(1)), int(m.group(2))


def session_year_label(year: int) -> str:
    """The session year as the House writes it: ``2026`` → ``"2026-2027"``."""
    return f"{int(year)}-{int(year) + 1}"


def period_of(session: str) -> Optional[int]:
    """The period a session belongs to, ``None`` for a sitting before the first known term."""
    if not _SESSION_RE.match(session):
        return None
    periods = [p for p, first in TERM_FIRST_SESSION.items() if first <= session]
    return max(periods) if periods else None


def session_number(period: int, session: str) -> int:
    """The period-unique integer for Stage 2 ``session.number``."""
    year, number = parse_session_key(session)
    return (year - int(period)) * 1000 + number


def session_years(period: int, last_year: int) -> list[int]:
    """The session years a term touches, up to the one starting in ``last_year``."""
    first_year, _ = parse_session_key(TERM_FIRST_SESSION[int(period)])
    later = [parse_session_key(first)[0] for p, first in TERM_FIRST_SESSION.items() if p > int(period)]
    end = min(later) if later else int(last_year)
    return list(range(first_year, end + 1))


def in_scope(args, session: str) -> bool:
    """Whether a run with ``args`` covers ``session``: its period, unless ``--no-limit-to-period``, and ``--limit-session``."""
    if not _SESSION_RE.match(session):
        return False
    if getattr(args, "limit_to_period", True) and period_of(session) != getattr(args, "period", None):
        return False
    limit = getattr(args, "limit_session", "")
    return not limit or bool(re.match(limit, session))


class Config(BaseConfig):
    # A session is present once the scraper wrote its Debat Direct file.
    MEDIA_GLOB_SUFFIX = "-debatdirect.json"
    # A speech the report has timed but not yet written out (interim versions)
    # stays as a clip without text; the session is text-bearing as long as any
    # speech has text.
    TEXT_PROBE = "any"

    def raw_sitting(self, session: str) -> Path:
        """The sitting's open data record and the report version that was fetched."""
        return self.dir('proceedings') / f"{session}-vergadering.json"

    def raw_report(self, session: str) -> Path:
        """The report XML of the sitting."""
        return self.dir('proceedings') / f"{session}-verslag.xml"

    def raw_debates(self, session: str) -> Path:
        """The Debat Direct records of the sitting's debates."""
        return self.dir('media') / f"{session}-debatdirect.json"
