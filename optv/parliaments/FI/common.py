#! /usr/bin/env python3
"""Config + path layout + session-key helpers for the FI (Eduskunta) pipeline.

Period semantics
----------------
``--period`` is the **vaalikausi (electoral term) start year** (e.g. ``2023``
for the 2023–2027 term). The Finnish parliament keys plenary sessions by
*valtiopäivät* (parliamentary year) + a per-year ``IstuntoNumero`` — e.g.
``2026/58`` — and that numbering resets each year while the term spans several
years. The Stage 2 model has a single ``electoralPeriod.number`` + a single
integer ``session.number``, so:

- ``electoralPeriod.number`` = the term start year (``2023``).
- The on-disk / ``meta.session`` **session key** is the string
  ``f"{year}-{number:03d}"`` (e.g. ``"2026-058"``).
- Stage 2 ``session.number`` (integer, must be unique within the period) is
  encoded ``(year - period) * 1000 + number`` so the per-year numbering does
  not collide across the years of a term (same composite-encoding trick as
  TW/NO). The raw ``{year}/{number}`` is preserved in ``meta.sourceLabel``.

This encoding shares a documented slot-width mismatch with SE.
"""

import json
import logging
import re
from pathlib import Path

# Re-exported for back-compat with the publish helper shape used by the shared
# workflow runner.
from optv.shared.publish import (  # noqa: F401
    data_signature,
    save_if_changed,
    data_has_timing,
    data_has_ner,
    is_demotion,
    carry_forward_wids,
    carry_forward_enrichments,
)
from optv.shared.config import BaseConfig
from optv.shared.session_status import SessionStatus  # noqa: F401

logger = logging.getLogger(__name__)

# A Finnish electoral term covers up to five valtiopäivät years (a partial
# election year at each end). The download stage iterates these to discover
# sessions; ``session_in_scope`` accepts any session whose year falls in range.
TERM_LENGTH_YEARS = 5

_SESSION_RE = re.compile(r"^(\d{4})-(\d{1,3})$")


def term_years(period: int) -> list[int]:
    """Valtiopäivät years belonging to the term starting in ``period``."""
    return [period + i for i in range(TERM_LENGTH_YEARS)]


def session_str(year: int, number: int) -> str:
    """Canonical on-disk / ``meta.session`` key, e.g. ``(2026, 58) → "2026-058"``."""
    return f"{year}-{int(number):03d}"


def parse_session_str(session: str) -> tuple[int, int]:
    """Inverse of :func:`session_str`. ``"2026-058" → (2026, 58)``."""
    m = _SESSION_RE.match(session)
    if not m:
        raise ValueError(f"Not an FI session key: {session!r} (expected YYYY-NNN)")
    return int(m.group(1)), int(m.group(2))


def session_number_int(period: int, year: int, number: int) -> int:
    """Encode the per-year session number into a period-unique integer.

    ``(year - period) * 1000 + number`` — keeps years of a term from colliding
    while staying within four digits (term ≤ 5 years, ≤ 999 sessions/year).
    """
    return (year - period) * 1000 + int(number)


class Config(BaseConfig):
    def __init__(self, data_dir: Path, cache_dir: Path = None):
        data_dir = Path(data_dir)
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
        else:
            cache_dir = data_dir / "cache"
        self._dir = {
            'data': data_dir,
            'cache': cache_dir,
            'media': data_dir / "original" / "media",
            'proceedings': data_dir / "original" / "proceedings",
            'merged': cache_dir / "merged",
            'aligned': cache_dir / "aligned",
            'ner': cache_dir / "ner",
            'audio': cache_dir / "audio",
            'audio_session': cache_dir / "audio_session",
            'processed': data_dir / "processed",
            'nel_data': data_dir / "metadata",
        }

    def file(self, session: str, stage: str = 'processed', create: bool = False) -> Path:
        suffix = stage
        d = self._dir[stage]
        if stage == 'processed':
            suffix = 'session'
        if create and not d.is_dir():
            d.mkdir(parents=True)
        return d / f"{session}-{suffix}.json"

    def raw_ptk(self, session: str) -> Path:
        """Raw PTK plenary-minutes XML downloaded from VaskiData."""
        return self.dir('proceedings') / f"{session}-ptk.xml"

    def raw_event(self, session: str) -> Path:
        """Raw verkkolähetys event (speakers[] + media URLs) extracted from the page."""
        return self.dir('media') / f"{session}-event.json"

    def save_data(self, data, session: str, stage: str) -> Path:
        outfile = self.file(session, stage)
        if not outfile.parent.is_dir():
            outfile.parent.mkdir(parents=True)
        with open(outfile, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return outfile

    def sessions(self, prefix: str = ''):
        """Enumerate sessions present on disk.

        Built from the raw verkkolähetys event files written by the scraper
        (``{session}-event.json``); media is the merge spine so a session is
        "present" once its video metadata is downloaded.
        """
        suffix = '-event.json'
        return list(sorted(
            f.name[:-len(suffix)]
            for f in self.dir('media').glob(f'{prefix}*{suffix}')
        ))

    def status(self, session: str) -> set:
        status = set()
        if self.file(session, 'media').exists():
            status.add(SessionStatus.media)
        if self.file(session, 'proceedings').exists():
            status.add(SessionStatus.proceedings)
        if self.file(session, 'merged').exists():
            status.add(SessionStatus.merged)
        sfile = self.file(session, 'processed')
        if sfile.exists():
            status.add(SessionStatus.session)
            with open(sfile) as f:
                info = json.load(f)
            data = info.get('data') or []
            if not data:
                status.add(SessionStatus.empty)
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            for s in data:
                tcs = s.get('textContents') or []
                if not tcs or not any((tc.get('textBody') or []) for tc in tcs):
                    status.add(SessionStatus.no_text)
                    return status
                if s.get('debug', {}).get('align-duration'):
                    status.add(SessionStatus.aligned)
                if s.get('debug', {}).get('ner-duration'):
                    status.add(SessionStatus.ner)
        return status


if __name__ == '__main__':
    import sys
    config = Config(Path(sys.argv[1]))
    import IPython
    IPython.embed()
