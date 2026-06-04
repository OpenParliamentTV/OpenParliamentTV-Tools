#! /usr/bin/env python3
"""PT Assembleia da República common config.

Session-key convention: ``{legislatura}-{sessão legislativa}-{reunião:03d}``,
e.g. ``17-1-059``. The pieces map onto the av.parlamento.pt path
``/videos/Plenary/{leg}/{sl}/{meeting}`` and the JSON API
``/api/v1/videos/Plenary/{leg}/{sl}/{meeting}``:

- ``legislatura`` (17) — the sequential parliamentary term → ``electoralPeriod.number``.
- ``sessão legislativa`` (1) — the parliamentary *year* within the term; the
  reunião number restarts each year.
- ``reunião`` (59) — the plenary meeting number within that year.

One Stage 2 session represents one *reunião plenária* (a calendar-day sitting).
Because the reunião number resets each sessão legislativa, ``session.number`` is
encoded so it stays unique within the legislatura — see
:func:`session_number_int`.

``electoralPeriod.number`` stays a clean sequential integer (17), like FR — the
dropped intra-term level (sessão legislativa) goes into the ``session.number``
encoding and ``meta.sourceLabel`` rather than overwriting the term.
"""

import logging
logger = logging.getLogger(__name__)

import json
import re
from pathlib import Path

# Re-exported for back-compat with shared callers.
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

# 17-1-059  →  (17, 1, 59)
_KEY_RE = re.compile(r"^(?P<leg>\d+)-(?P<sl>\d+)-(?P<meeting>\d+)$")


def make_session(leg: int, sl: int, meeting: int) -> str:
    """``(17, 1, 59)`` → session key ``17-1-059``."""
    return f"{int(leg)}-{int(sl)}-{int(meeting):03d}"


def parse_session(session: str) -> tuple[int, int, int]:
    """Return ``(legislatura, sessão_legislativa, reunião)`` for a session key."""
    m = _KEY_RE.match(session)
    if not m:
        raise ValueError(f"unrecognized session key: {session!r}")
    return int(m["leg"]), int(m["sl"]), int(m["meeting"])


def session_number_int(session: str) -> int:
    """Encode the session key into a legislatura-unique integer.

    ``sessão_legislativa * 1000 + reunião`` — e.g. ``17-1-059`` → ``1059``,
    ``17-2-059`` → ``2059``. The reunião number restarts each sessão legislativa
    (so SL1 reunião 59 and SL2 reunião 59 would otherwise collide); multiplying
    the SL by 1000 keeps each year's meetings in their own band. Meeting numbers
    stay well below 1000. Same composite-encoding class as FR / FI / TW — fits the
    platform's 4-digit session slot for legislatura 17 (SL ≤ 4, meeting ≤ ~200).
    """
    _leg, sl, meeting = parse_session(session)
    return sl * 1000 + meeting


def source_label(session: str) -> str:
    leg, sl, meeting = parse_session(session)
    return f"L{leg} SL{sl} Reunião {meeting:03d}"


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

    def file(self, session: str, stage: str = 'processed', create=False) -> Path:
        suffix = stage
        d = self._dir[stage]
        if stage == 'processed':
            suffix = 'session'
        if create and not d.is_dir():
            d.mkdir(parents=True)
        return d / f"{session}-{suffix}.json"

    def raw_av(self, session: str) -> Path:
        """Raw av.parlamento.pt per-meeting JSON (media spine) for one reunião."""
        return self.dir('media') / f"{session}-av.json"

    def raw_dar(self, session: str) -> Path:
        """Raw debates.parlamento.pt full-text (``?sft=true``) HTML for one reunião."""
        return self.dir('proceedings') / f"{session}-dar.html"

    def data(self, session: str, stage: str = 'processed') -> list:
        filename = self.file(session, stage)
        if filename.exists():
            with open(filename) as f:
                data = json.load(f)
        else:
            logger.warning(f"No data for {session}-{stage}")
            data = []
        return data

    def sessions(self, prefix: str = ''):
        """Return the list of currently existing sessions.

        Built from the raw av.parlamento.pt JSON files written by the scraper
        (``{session}-av.json``); the av interventions are the merge spine.
        """
        suffix = '-av.json'
        return list(sorted(
            f.name[:-len(suffix)]
            for f in self.dir('media').glob(f'{prefix}*{suffix}')
        ))

    def status(self, session: str) -> set:
        """Return the status for the given session."""
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
            with open(sfile, 'r') as f:
                data = json.load(f)['data']
            if len(data) == 0:
                status.add(SessionStatus.empty)
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            for s in data:
                tcs = s.get('textContents') or []
                if not tcs or not any((tc.get('textBody') or []) for tc in tcs):
                    status.add(SessionStatus.no_text)
                    continue
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
