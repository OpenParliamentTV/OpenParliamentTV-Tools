#! /usr/bin/env python3

"""DE-HH file layout and session-status helpers.

Mirrors the DE-SH / DE-BY / DE-BW Config shape: ``original/{media,proceedings}``
for raw downloads, ``cache/{merged,aligned,ner}`` for per-stage outputs,
``processed/`` for the published Stage 2 files, ``metadata/`` for the NEL
entity dump and the session index.

DE-HH sessions are keyed by Sitzung number combined with the Wahlperiode,
e.g. ``23018`` = WP 23, 18. Sitzung. Sitzung is the canonical unit for
Plenarprotokoll citations. Like DE-BY / DE-BW (and unlike DE-SH's Tagung /
DE-ST's Sitzungsperiode), the Hamburgische Bürgerschaft has a **pure two-level**
hierarchy (Wahlperiode > Sitzung) — there is no intra-term grouping level to
drop, so there is no ``meta.tagung`` analogue.
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
