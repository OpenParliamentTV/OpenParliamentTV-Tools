#! /usr/bin/env python3

import logging
logger = logging.getLogger(__name__)

from pathlib import Path

from optv.shared.config import BaseConfig

# Re-exported for back-compat: existing callers do
# `from .common import SessionStatus, data_signature, is_demotion, ...`.
# The Conductor imports `optv.parliaments.DE.common.SessionStatus` in-process.
from optv.shared.publish import (  # noqa: F401
    data_signature,
    save_if_changed,
    data_has_timing,
    data_has_ner,
    is_demotion,
    carry_forward_wids,
    carry_forward_enrichments,
)
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # DE media files are named `raw-{period}{NNN}-media.json`; the session key
    # is the 5-digit `{period}{NNN}` between the `raw-` prefix and the suffix.
    MEDIA_GLOB_PREFIX = "raw-"


if __name__ == '__main__':
    import sys
    config = Config(Path(sys.argv[1]))
    import IPython
    IPython.embed()

