#! /usr/bin/env python3

"""AT Nationalrat file-layout config.

Subclasses the shared :class:`optv.shared.config.BaseConfig`. The only
parliament-specific knob is the session-discovery glob: the scraper writes one
raw Mediathek payload per session as ``original/media/{session}-mediathek.json``,
so ``sessions()`` enumerates those. The parsed intermediate files keep the
canonical names (``{session}-media.json`` / ``{session}-proceedings.json``).

Session keys are ``{EP}{nnn}`` — electoral period 27 + zero-padded sitting
number, e.g. ``27144`` for the 144th sitting of the XXVII. Gesetzgebungsperiode.

The default ``status()`` probe is inherited unchanged: the merger stamps
``debug.proceedingIndex`` on text-bearing speeches, align stamps
``debug.alignDuration`` and NER stamps ``debug.nerDuration`` — the three keys the
base probe keys off.
"""

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # The scraper's per-session raw spine file is ``{session}-mediathek.json``.
    MEDIA_GLOB_SUFFIX = "-mediathek.json"
    # Media spine: Mediathek clips without a matching proceedings turn stay as
    # media-only records with empty textContents; the session is still aligned
    # and NER'd.
    TEXT_PROBE = "any"
