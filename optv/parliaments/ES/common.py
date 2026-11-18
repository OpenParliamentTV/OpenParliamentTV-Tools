#! /usr/bin/env python3

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # Media files are named `raw-{period}{NNN}-media.json`; the session key is
    # the 5-digit `{period}{NNN}` between the `raw-` prefix and the suffix.
    MEDIA_GLOB_PREFIX = "raw-"
    # Media spine: role-only chair turns and oaths leave a few clips per
    # session without matched text; the session is still aligned and NER'd.
    TEXT_PROBE = "any"
