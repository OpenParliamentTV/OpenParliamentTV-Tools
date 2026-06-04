#! /usr/bin/env python3
"""Config + path layout for the NO (Stortinget) pipeline."""

import json
import logging
from pathlib import Path

# Re-exported for back-compat with the publish helper shape used by other
# parliaments. ``carry_forward_*`` is used by ``optv.shared.workflow``.
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


# Stortingsperiode (4-year term) → OPTV-internal integer period.
# We can't use the API's string period ("2025-2029") because the shared
# argparser requires ``--period`` to be an integer. 22 is the ordinal of
# the current Storting (the 22nd since 1814 numbering would be too messy);
# in practice the integer is opaque -- the session string and the manifest
# anchor the value, not the number itself.
TERM_TO_PERIOD = {
    "2025-2029": 22,
}
PERIOD_TO_TERM = {v: k for k, v in TERM_TO_PERIOD.items()}

# Norwegian Bokmål parliamentary sessions run a single year inside a term
# (Oct→Sept). We list each term's session-years explicitly so the download
# stage knows which `sesjonid` values to query for a given period.
TERM_TO_SESJONIDER = {
    "2025-2029": ["2025-2026", "2026-2027", "2027-2028", "2028-2029"],
}


def period_to_sesjonider(period: int) -> list[str]:
    term = PERIOD_TO_TERM.get(period)
    if not term:
        raise ValueError(f"Unknown NO period {period} - extend TERM_TO_PERIOD in common.py")
    return TERM_TO_SESJONIDER[term]


class Config(BaseConfig):
    def __init__(self, data_dir: Path, cache_dir: Path | None = None):
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
            'meetings': data_dir / "original" / "meetings",
            'merged': cache_dir / "merged",
            'aligned': cache_dir / "aligned",
            'ner': cache_dir / "ner",
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

    def save_data(self, data, session: str, stage: str) -> Path:
        outfile = self.file(session, stage)
        if not outfile.parent.is_dir():
            outfile.parent.mkdir(parents=True)
        with open(outfile, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return outfile

    def sessions(self, prefix: str = ''):
        """Enumerate sessions present on disk.

        Built from the per-meeting media files written by the scraper:
        ``<session>-media.json``. ``session`` strings have the form
        ``{period}_{moteid}`` (e.g. ``22_11518``).
        """
        suffix = '-media.json'
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
