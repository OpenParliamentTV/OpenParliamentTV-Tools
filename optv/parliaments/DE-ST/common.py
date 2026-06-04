#! /usr/bin/env python3

"""DE-ST file layout and session-status helpers.

Mirrors the DE-RP Config shape: ``original/{media,proceedings}`` for raw
downloads, ``cache/{merged,aligned,ner}`` for per-stage outputs,
``processed/`` for the published Stage 2 files, ``metadata/`` for the NEL
entity dump and the cumulative Sitzung map.

DE-ST sessions are keyed by Landtagssitzung number (e.g. ``08105``) — the
canonical reference unit for Plenarprotokolle and Drucksachen. The session
HTML on the portal is grouped per-Sitzungsperiode (``sp-NNN.html``), but
the parser splits each multi-day page into per-Sitzung intermediates.
"""

import logging
logger = logging.getLogger(__name__)

import json
from pathlib import Path

# Re-exports for back-compat with the shared workflow runner.
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

    def save_data(self, data, session: str, stage: str) -> Path:
        outfile = self.file(session, stage)
        if not outfile.parent.is_dir():
            outfile.parent.mkdir(parents=True)
        with open(outfile, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return outfile

    def sessions(self, prefix: str = ''):
        """Sessions are discovered from per-Sitzung media JSON files."""
        return list(sorted(
            f.name[: -len('-media.json')]
            for f in self.dir('media').glob(f'{prefix}*-media.json')
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
            data = info.get('data', [])
            if not data:
                status.add(SessionStatus.empty)
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            for s in data:
                if s.get('debug', {}).get('proceedingIndex') is None:
                    status.add(SessionStatus.no_text)
                    return status
                if s.get('debug', {}).get('align-duration'):
                    status.add(SessionStatus.aligned)
                if s.get('debug', {}).get('ner-duration'):
                    status.add(SessionStatus.ner)
        return status
