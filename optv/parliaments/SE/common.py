#! /usr/bin/env python3

import logging
logger = logging.getLogger(__name__)

import json
from pathlib import Path

# Re-exported for back-compat: existing callers do
# `from .common import SessionStatus, data_signature, is_demotion, ...`.
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


class Config(BaseConfig):
    def __init__(self, data_dir: Path,
                 cache_dir: Path = None):
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
            'processed': data_dir / "processed",
            'nel_data': data_dir / "metadata"
        }


    def file(self, session: str, stage: str = 'processed', create = False) -> Path:
        suffix = stage
        d = self._dir[stage]
        if stage == 'processed':
            suffix = 'session'
        if create:
            # Make sure the containing directory exists
            if not d.is_dir():
                d.mkdir(parents=True)
        return d / f"{session}-{suffix}.json"


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

        Built from the proceedings ``anforanden`` files written by the scraper:
        ``<period>-<protokoll_nr>-anforanden.json`` (e.g. ``2025-091-anforanden.json``).
        """
        suffix = '-anforanden.json'
        return list(sorted(
            f.name[:-len(suffix)]
            for f in self.dir('proceedings').glob(f'{prefix}*{suffix}')
        ))


    def status(self, session: str) -> set:
        """Return the status for the given session.

        Return set of SessionStatus flags.
        """
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
                info = json.load(f)
                data = info['data']
            if len(data) == 0:
                status.add(SessionStatus.empty)
            # Check for wid/wtype in people, in the first non-empty people list
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            # SE pre-joins text and video at the API; "no text" means a speech
            # has no textContents (or empty textBody).
            for s in data:
                tcs = s.get('textContents') or []
                if not tcs or not any((tc.get('textBody') or []) for tc in tcs):
                    status.add(SessionStatus.no_text)
                    return status
                if s.get('debug', {}).get('alignDuration'):
                    status.add(SessionStatus.aligned)
                if s.get('debug', {}).get('nerDuration'):
                    status.add(SessionStatus.ner)

        return status

if __name__ == '__main__':
    import sys
    config = Config(Path(sys.argv[1]))
    import IPython
    IPython.embed()
