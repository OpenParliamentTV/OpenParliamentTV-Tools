#! /usr/bin/env python3
"""PT Assembleia da República common config.

Session-key convention: ``{legislatura}-{sessão legislativa}-{reunião:03d}``,
e.g. ``17-1-059``. The pieces map onto the av.parlamento.pt path
``/videos/Plenary/{leg}/{sl}/{meeting}`` and the JSON API
``/api/v1/videos/Plenary/{leg}/{sl}/{meeting}``:

- ``legislatura`` (17) — the sequential parliamentary term → ``electoralPeriod.number``.
- ``sessão legislativa`` (1) — the parliamentary *year* within the term; the
  reunião number restarts each year.
- ``reunião`` (59) — the plenary meeting number within that year.

One Stage 2 session represents one *reunião plenária* (a calendar-day sitting).
Because the reunião number resets each sessão legislativa, ``session.number`` is
encoded so it stays unique within the legislatura — see
:func:`session_number_int`.

``electoralPeriod.number`` stays a clean sequential integer (17), like FR — the
dropped intra-term level (sessão legislativa) goes into the ``session.number``
encoding and ``meta.sourceLabel`` rather than overwriting the term.
"""

import re
from pathlib import Path

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401

# 17-1-059  →  (17, 1, 59)
_KEY_RE = re.compile(r"^(?P<leg>\d+)-(?P<sl>\d+)-(?P<meeting>\d+)$")


def make_session(leg: int, sl: int, meeting: int) -> str:
    """``(17, 1, 59)`` → session key ``17-1-059``."""
    return f"{int(leg)}-{int(sl)}-{int(meeting):03d}"


def parse_session(session: str) -> tuple[int, int, int]:
    """Return ``(legislatura, sessão_legislativa, reunião)`` for a session key."""
    m = _KEY_RE.match(session)
    if not m:
        raise ValueError(f"unrecognized session key: {session!r}")
    return int(m["leg"]), int(m["sl"]), int(m["meeting"])


def session_number_int(session: str) -> int:
    """Encode the session key into a legislatura-unique integer.

    ``sessão_legislativa * 1000 + reunião`` — e.g. ``17-1-059`` → ``1059``,
    ``17-2-059`` → ``2059``. The reunião number restarts each sessão legislativa
    (so SL1 reunião 59 and SL2 reunião 59 would otherwise collide); multiplying
    the SL by 1000 keeps each year's meetings in their own band. Meeting numbers
    stay well below 1000. Same composite-encoding class as FR / FI / TW — fits the
    platform's 4-digit session slot for legislatura 17 (SL ≤ 4, meeting ≤ ~200).
    """
    _leg, sl, meeting = parse_session(session)
    return sl * 1000 + meeting


def source_label(session: str) -> str:
    leg, sl, meeting = parse_session(session)
    return f"L{leg} SL{sl} Reunião {meeting:03d}"


class Config(BaseConfig):
    # The av interventions are the spine: an intervention without a DAR text
    # match keeps empty textContents, and text shows as non-empty
    # textContents (no debug.proceedingIndex). A session is text-bearing
    # when any speech has text; align and NER skip the others.
    TEXT_SIGNAL = "textContents"
    TEXT_PROBE = "any"
    # Sessions are discovered from the raw av.parlamento.pt JSON files written by
    # the scraper (``{session}-av.json``); the av interventions are the merge
    # spine.
    MEDIA_GLOB_SUFFIX = "-av.json"
    EXTRA_DIRS = {
        "opendata": ("data", "original/opendata"),
        "opendata_cache": ("cache", "opendata"),
    }

    def raw_av(self, session: str) -> Path:
        """Raw av.parlamento.pt per-meeting JSON (media spine) for one reunião."""
        return self.dir('media') / f"{session}-av.json"

    def raw_av_metadata(self, session: str) -> Path:
        """av.parlamento.pt per-intervention summaries (``…/metadata/json``)."""
        return self.dir('media') / f"{session}-av-metadata.json"

    def raw_av_sources(self, session: str) -> Path:
        """Recording URLs from the av meeting page (``meetingVideoSources``)."""
        return self.dir('media') / f"{session}-av-sources.json"

    def raw_od(self, session: str) -> Path:
        """This reunião's records from the open data Intervenções file."""
        return self.dir('opendata') / f"{session}-od.json"

    def raw_dar(self, session: str) -> Path:
        """Raw debates.parlamento.pt full-text (``?sft=true``) HTML for one reunião."""
        return self.dir('proceedings') / f"{session}-dar.html"
