#! /usr/bin/env python3
"""EU European Parliament common config.

Session-key convention: ``YYYYMMDD`` (date of one plenary day; EP publishes
one CRE verbatim per day, spanning 2-3 sittings each with its own HLS event
reference). Same key drives cache/published filenames, e.g.
``20251008-merged.json``, ``20251008-session.json``.
"""

import logging
logger = logging.getLogger(__name__)

import json
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
            'audio': cache_dir / "audio",
            'audio_debate': cache_dir / "audio_debate",
            'processed': data_dir / "processed",
            'nel_data': data_dir / "metadata"
        }


    def file(self, session: str, stage: str = 'processed', create = False) -> Path:
        suffix = stage
        d = self._dir[stage]
        if stage == 'processed':
            suffix = 'session'
        if create:
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

        Built from the per-day CRE index files written by the scraper:
        ``raw-{YYYYMMDD}-cre.json``. Session keys are the bare YYYYMMDD date.
        """
        suffix = '-cre.json'
        cre_prefix = 'raw-'
        return list(sorted(
            f.name[len(cre_prefix):-len(suffix)]
            for f in self.dir('proceedings').glob(f'{cre_prefix}{prefix}*{suffix}')
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
                info = json.load(f)
                data = info['data']
            if len(data) == 0:
                status.add(SessionStatus.empty)
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            # EU speeches always have text from CRE; "no_text" means a speech
            # has no textContents (or empty textBody) — same shape as SE.
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
