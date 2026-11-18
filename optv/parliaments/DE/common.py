#! /usr/bin/env python3

from optv.shared.config import BaseConfig
# The Conductor imports `optv.parliaments.DE.common.SessionStatus` in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # DE media files are named `raw-{period}{NNN}-media.json`; the session key
    # is the 5-digit `{period}{NNN}` between the `raw-` prefix and the suffix.
    MEDIA_GLOB_PREFIX = "raw-"
