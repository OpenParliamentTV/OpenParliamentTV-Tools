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

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    # The Plenar-TV WebVTT subtitles are attached to the video spine by
    # speechIndex, so sessions carry text. Sessions are discovered from bare
    # ``{session}-media.json`` files (BaseConfig defaults).
    # Media spine: clips without subtitle text keep empty textContents; the
    # session is still aligned and NER'd.
    TEXT_PROBE = "any"
