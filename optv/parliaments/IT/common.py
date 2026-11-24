#! /usr/bin/env python3
"""IT Camera dei deputati: file layout and session keys.

A session is one sitting (*seduta*) of the Chamber's assembly. The Chamber numbers its sittings within a legislature, so the session key is ``{legislature}{sitting:04d}``, e.g. ``190731`` for sitting 731 of the XIX legislature. Four digits is the width the Chamber itself writes (``sed0731``). ``--period`` is the legislature.
"""

import re
from pathlib import Path

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401

_SESSION_RE = re.compile(r"^(\d{2})(\d{4})$")


def session_key(period: int, number: int) -> str:
    """``(19, 731)`` → ``"190731"``."""
    return f"{int(period):02d}{int(number):04d}"


def parse_session_key(session: str) -> tuple[int, int]:
    """``"190731"`` → ``(19, 731)``."""
    m = _SESSION_RE.match(session)
    if not m:
        raise ValueError(f"Not an IT session key: {session!r} (expected LLNNNN, e.g. 190731)")
    return int(m.group(1)), int(m.group(2))


def in_scope(args, session: str) -> bool:
    """Whether a run with ``args`` covers ``session``: its period, unless ``--no-limit-to-period``, and ``--limit-session``."""
    m = _SESSION_RE.match(session)
    if not m:
        return False
    period = getattr(args, "period", None)
    if getattr(args, "limit_to_period", True) and period is not None and int(m.group(1)) != int(period):
        return False
    limit = getattr(args, "limit_session", "")
    return not limit or bool(re.match(limit, session))


class Config(BaseConfig):
    # A session is present once the scraper wrote its WebTV page.
    MEDIA_GLOB_SUFFIX = "-evento.html"
    # An intervention the record has no text for (a suspension, the reading of
    # the minutes) stays a clip without text; the session is text-bearing as
    # long as any speech has text.
    TEXT_PROBE = "any"

    def raw_event(self, session: str) -> Path:
        """The sitting's page on the Chamber's WebTV."""
        return self.dir('media') / f"{session}-evento.html"

    def raw_archive_entry(self, session: str) -> Path:
        """The sitting's entry in the WebTV archive, and what was fetched for it."""
        return self.dir('media') / f"{session}-archivio.json"

    def raw_record(self, session: str) -> Path:
        """The XML version of the sitting's stenographic record."""
        return self.dir('proceedings') / f"{session}-stenografico.xml"
