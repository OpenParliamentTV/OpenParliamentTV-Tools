#! /usr/bin/env python3

"""DE-NI file layout and session-status helpers.

Mirrors the DE-HH / DE-SH / DE-BY / DE-BW Config shape:
``original/{media,proceedings}`` for raw downloads, ``cache/{merged,aligned,ner}``
for per-stage outputs, ``processed/`` for the published Stage 2 files,
``metadata/`` for the NEL entity dump and the session index.

DE-NI sessions are keyed by Sitzung number combined with the Wahlperiode,
e.g. ``19080`` = WP 19, 80. Sitzung. Sitzung is the canonical unit for
Plenarprotokoll citations. Lower Saxony nests **Wahlperiode > Tagungsabschnitt
> Sitzung** (Plenar-TV calls a Tagungsabschnitt a "session" and a Sitzung a
"meeting"), so unlike DE-HH's pure two-level hierarchy there is a dropped
intra-term super-level (the Tagungsabschnitt). The Sitzung number is unique
within a Wahlperiode, so the ``{WP:02d}{Sitzung:03d}`` key stays flat; the
Tagungsabschnitt is carried in ``meta``/``debug``.
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
