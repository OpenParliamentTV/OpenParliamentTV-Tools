#! /usr/bin/env python3
"""Per-parliament config + on-disk layout for Taiwan's Legislative Yuan (TW).

Session keys are ``{屆:02d}{會期:02d}{會次:03d}`` strings (e.g. ``"1105011"`` =
term 11, session-period 5, plenary meeting 11 → meeting code ``院會-11-5-11``).
The fixed-width prefix is what makes ``--period=11`` work via the shared
``session.startswith(str(args.period))`` filter.
"""

import logging
import json
from pathlib import Path

# Re-exported for back-compat with the other parliaments' common modules.
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


def encode_session(term: int, session_period: int, meeting_number: int) -> str:
    """Encode a TW plenary meeting code as the OPTV session string.

    ``院會-11-5-11`` → ``"1105011"``. The width is fixed so the resulting
    string sorts chronologically inside one term.
    """
    return f"{term:02d}{session_period:02d}{meeting_number:03d}"


def decode_session(session: str) -> tuple[int, int, int]:
    """Inverse of :func:`encode_session`. Raises ValueError on bad input."""
    if len(session) != 7 or not session.isdigit():
        raise ValueError(f"Bad TW session key {session!r}; want 7-digit string.")
    return int(session[0:2]), int(session[2:4]), int(session[4:7])


def session_meeting_code(session: str) -> str:
    """``"1105011"`` → ``"院會-11-5-11"``."""
    term, sp, mn = decode_session(session)
    return f"院會-{term}-{sp}-{mn}"


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
            # Raw downloads share the original/media + original/proceedings
            # directories but have distinct filename suffixes — see
            # Config.sessions() docstring.
            'ivods': data_dir / "original" / "media",
            'details': data_dir / "original" / "proceedings",
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
        with open(outfile, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return outfile

    def sessions(self, prefix: str = ''):
        """Return on-disk session keys, discovered from raw IVOD downloads.

        Raw downloads land at ``original/media/{session}-ivods.json``; the
        parsed media file at ``original/media/{session}-media.json`` (a
        derivative) and the proceedings detail at
        ``original/proceedings/{session}-details.json``.
        """
        suffix = '-ivods.json'
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
            data = info.get('data', [])
            if not data:
                status.add(SessionStatus.empty)
                return status
            if any(s.get('people') and s['people'][0].get('wid') for s in data):
                status.add(SessionStatus.linked)
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
