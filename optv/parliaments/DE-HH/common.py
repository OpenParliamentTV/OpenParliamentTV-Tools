#! /usr/bin/env python3

"""DE-HH file layout and session-status helpers.

Mirrors the DE-SH / DE-BY / DE-BW Config shape: ``original/{media,proceedings}``
for raw downloads, ``cache/{merged,aligned,ner}`` for per-stage outputs,
``processed/`` for the published Stage 2 files, ``metadata/`` for the NEL
entity dump and the session index. DE-HH adds ``original/subtitles/{sid}/`` for
the per-speech WebVTT files of the mediathek.

DE-HH sessions are keyed by Sitzung number combined with the Wahlperiode,
e.g. ``23018`` = WP 23, 18. Sitzung. Sitzung is the canonical unit for
Plenarprotokoll citations. Like DE-BY / DE-BW (and unlike DE-SH's Tagung /
DE-ST's Sitzungsperiode), the Hamburgische Bürgerschaft has a **pure two-level**
hierarchy (Wahlperiode > Sitzung) — there is no intra-term grouping level to
drop, so there is no ``meta.tagung`` analogue.
"""

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # Sessions carry text: the Plenarprotokoll PDF (pdf2tei, joined onto the
    # video spine) once it is published, the mediathek subtitles until then.
    # Sessions are discovered from bare ``{session}-media.json`` files
    # (BaseConfig defaults).
    # Media spine: clips without text keep empty textContents; the session is
    # still aligned and NER'd.
    TEXT_PROBE = "any"
    # Per-speech WebVTT files of the mediathek, ``subtitles/{session}/``.
    EXTRA_DIRS = {"subtitles": ("data", "original/subtitles")}
