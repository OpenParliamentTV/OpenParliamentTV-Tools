#! /usr/bin/env python3
"""Config + path layout + session-key helpers for the FI (Eduskunta) pipeline.

Period semantics
----------------
``--period`` is the **vaalikausi (electoral term) start year** (e.g. ``2023``
for the 2023–2027 term). The Finnish parliament keys plenary sessions by
*valtiopäivät* (parliamentary year) + a per-year ``IstuntoNumero`` — e.g.
``2026/58`` — and that numbering resets each year while the term spans several
years. The Stage 2 model has a single ``electoralPeriod.number`` + a single
integer ``session.number``, so:

- ``electoralPeriod.number`` = the term start year (``2023``).
- The on-disk / ``meta.session`` **session key** is the string
  ``f"{year}-{number:03d}"`` (e.g. ``"2026-058"``).
- Stage 2 ``session.number`` (integer, must be unique within the period) is
  encoded ``(year - period) * 1000 + number`` so the per-year numbering does
  not collide across the years of a term (same composite-encoding trick as
  TW/NO). The raw ``{year}/{number}`` is preserved in ``meta.sourceLabel``.

This encoding shares a documented slot-width mismatch with SE.
"""

import re
from pathlib import Path

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


# A Finnish electoral term covers up to five valtiopäivät years (a partial
# election year at each end). The download stage iterates these to discover
# sessions; ``session_in_scope`` accepts any session whose year falls in range.
TERM_LENGTH_YEARS = 5

_SESSION_RE = re.compile(r"^(\d{4})-(\d{1,3})$")


def term_years(period: int) -> list[int]:
    """Valtiopäivät years belonging to the term starting in ``period``."""
    return [period + i for i in range(TERM_LENGTH_YEARS)]


def session_str(year: int, number: int) -> str:
    """Canonical on-disk / ``meta.session`` key, e.g. ``(2026, 58) → "2026-058"``."""
    return f"{year}-{int(number):03d}"


def parse_session_str(session: str) -> tuple[int, int]:
    """Inverse of :func:`session_str`. ``"2026-058" → (2026, 58)``."""
    m = _SESSION_RE.match(session)
    if not m:
        raise ValueError(f"Not an FI session key: {session!r} (expected YYYY-NNN)")
    return int(m.group(1)), int(m.group(2))


def session_number_int(period: int, year: int, number: int) -> int:
    """Encode the per-year session number into a period-unique integer.

    ``(year - period) * 1000 + number`` — keeps years of a term from colliding
    while staying within four digits (term ≤ 5 years, ≤ 999 sessions/year).
    """
    return (year - period) * 1000 + int(number)


class Config(BaseConfig):
    # Media is the spine: a broadcast clip without a matching speech record keeps
    # empty textContents, and text shows as non-empty textContents. A
    # session is text-bearing when any speech has text; align and NER skip
    # the others.
    TEXT_SIGNAL = "textContents"
    TEXT_PROBE = "any"
    # Sessions are discovered from the raw verkkolähetys event files written by
    # the scraper (``{session}-event.json``); media is the merge spine so a
    # session is "present" once its video metadata is downloaded.
    MEDIA_GLOB_SUFFIX = "-event.json"

    def raw_speeches(self, session: str) -> Path:
        """Raw speech records of the sitting from the open data API search."""
        return self.dir('proceedings') / f"{session}-speeches.json"

    def raw_event(self, session: str) -> Path:
        """Raw verkkolähetys event (speakers[] + media URLs) extracted from the page."""
        return self.dir('media') / f"{session}-event.json"
