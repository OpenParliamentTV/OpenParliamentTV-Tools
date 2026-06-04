#! /usr/bin/env python3

"""DE-SH file layout and session-status helpers.

Mirrors the DE-RP / DE-ST Config shape: ``original/{media,proceedings}``
for raw downloads, ``cache/{merged,aligned,ner}`` for per-stage outputs,
``processed/`` for the published Stage 2 files, ``metadata/`` for the NEL
entity dump and the date-to-Sitzung map.

DE-SH sessions are keyed by Landtagssitzung number combined with the
Wahlperiode, e.g. ``20119`` = WP 20 Sitzung 119. Sitzung is the canonical
unit for Plenarprotokoll citations and the sequential counter used in the
PDF URL pattern (``20-NNN_MM-YY.pdf``). The Tagung level (a multi-day
plenary block grouping several Landtagssitzungen) is recorded only in
``meta.tagung`` and ``debug.*``.
"""

import logging
logger = logging.getLogger(__name__)

import json
from pathlib import Path

# Re-exports for back-compat with the shared workflow runner.
from optv.shared.config import BaseConfig

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
    # Video-only: PDF-only/absent proceedings, no `align`/`ner` stages, so the
    # session status short-circuits to ``no_text``. Sessions are discovered
    # from bare ``{session}-media.json`` files (BaseConfig defaults).
    HAS_TEXT = False
