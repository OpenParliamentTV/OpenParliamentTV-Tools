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

import logging
from pathlib import Path

# Re-exported for back-compat: callers do
# ``from .common import SessionStatus, save_if_changed, …``.
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

logger = logging.getLogger(__name__)


class Config(BaseConfig):
    # The scraper's per-session raw spine file is ``{session}-mediathek.json``.
    MEDIA_GLOB_SUFFIX = "-mediathek.json"


if __name__ == '__main__':
    import sys
    config = Config(Path(sys.argv[1]))
    import IPython
    IPython.embed()
