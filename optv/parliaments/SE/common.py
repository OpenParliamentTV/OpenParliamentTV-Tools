#! /usr/bin/env python3

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # Media is the spine: a media entry without a matching anförande keeps
    # empty textContents, and text shows as non-empty textContents. A
    # session is text-bearing when any speech has text; align and NER skip
    # the others.
    TEXT_SIGNAL = "textContents"
    TEXT_PROBE = "any"
    # Sessions are discovered from the proceedings ``anforanden`` files written
    # by the scraper: ``<period>-<protokoll_nr>-anforanden.json`` (e.g.
    # ``2025-091-anforanden.json``).
    SESSION_SOURCE = "proceedings"
    MEDIA_GLOB_SUFFIX = "-anforanden.json"
