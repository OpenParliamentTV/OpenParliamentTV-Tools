#! /usr/bin/env python3
"""EU European Parliament common config.

Session-key convention: ``YYYYMMDD`` (date of one plenary day; EP publishes
one CRE verbatim per day, spanning 2-3 sittings each with its own HLS event
reference). Same key drives cache/published filenames, e.g.
``20251008-merged.json``, ``20251008-session.json``.
"""

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # Sessions are discovered from the per-day CRE index files written by the
    # scraper: ``raw-{YYYYMMDD}-cre.json``. Session keys are the bare YYYYMMDD
    # date.
    SESSION_SOURCE = "proceedings"
    MEDIA_GLOB_PREFIX = "raw-"
    MEDIA_GLOB_SUFFIX = "-cre.json"
    # EU speeches always have text from the CRE; a speech without a non-empty
    # textBody makes the session ``no_text``.
    TEXT_SIGNAL = "textContents"
