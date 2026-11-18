#! /usr/bin/env python3

"""DE-BW file layout and session-status helpers.

Mirrors the DE-SH / DE-BY / DE-RP Config shape: ``original/{media,proceedings}``
for raw downloads, ``cache/{merged,aligned,ner}`` for per-stage outputs,
``processed/`` for the published Stage 2 files, ``metadata/`` for the NEL
entity dump and the archive index.

DE-BW sessions are keyed by Sitzung number combined with the Wahlperiode,
e.g. ``17118`` = WP 17, 118. Sitzung. Sitzung is the canonical unit for
Plenarprotokoll citations. Like DE-BY (and unlike DE-SH's Tagung / DE-ST's
Sitzungsperiode), the Landtag von Baden-Württemberg has a **pure two-level**
hierarchy (Wahlperiode > Sitzung) — there is no intra-term grouping level to
drop, so there is no ``meta.tagung`` analogue.
"""

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # The Plenarprotokoll PDFs are parsed (pdf2tei) and joined onto the video
    # spine, so sessions carry proceedings text. Sessions are discovered from
    # bare ``{session}-media.json`` files (BaseConfig defaults).
    # Media spine: clips without a protocol text match keep empty
    # textContents; the session is still aligned and NER'd.
    TEXT_PROBE = "any"
