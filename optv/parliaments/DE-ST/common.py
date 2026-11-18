#! /usr/bin/env python3

"""DE-ST file layout and session-status helpers.

Mirrors the DE-RP Config shape: ``original/{media,proceedings}`` for raw
downloads, ``cache/{merged,aligned,ner}`` for per-stage outputs,
``processed/`` for the published Stage 2 files, ``metadata/`` for the NEL
entity dump and the per-period Sitzung map.

DE-ST sessions are keyed by Landtagssitzung number (e.g. ``09002``) — the
canonical reference unit for Plenarprotokolle and Drucksachen. The session
HTML on the portal is grouped per-Sitzungsperiode (``sp-PP-NNN.html``), but
the parser splits each multi-day page into per-Sitzung intermediates.
"""

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


class Config(BaseConfig):
    """The shared layout and status probe apply unchanged; sessions are discovered from the per-Sitzung ``{session}-media.json`` files."""
