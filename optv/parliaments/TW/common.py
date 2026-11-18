#! /usr/bin/env python3
"""Per-parliament config + on-disk layout for Taiwan's Legislative Yuan (TW).

Session keys are ``{屆:02d}{會期:02d}{會次:03d}`` strings (e.g. ``"1105011"`` =
term 11, session-period 5, plenary meeting 11 → meeting code ``院會-11-5-11``).
The fixed-width prefix is what makes ``--period=11`` work via the shared
``session.startswith(str(args.period))`` filter.
"""

import logging
import json
import re
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

# Official per-clip text (gazette record) on the IVOD site; empty until the
# gazette is published, weeks after the sitting.
SPEECH_RECORD_URL = "https://ivod.ly.gov.tw/Demand/Speech/{ivod_id}"

# Session keys whose already-published file lacks newly arrived official text;
# the workflow re-runs them with rebuild semantics (see scraper/fetch_records.py).
OFFICIAL_TEXT_UPGRADES_FILE = "tw-official-text-upgrades.txt"


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


def sessions_in_scope(config: "Config", args) -> list[str]:
    """On-disk session keys the download steps should touch.

    Mirrors the shared runner's default scope: ``--period`` prefix (unless
    ``--no-limit-to-period``) and the ``--limit-session`` regex.
    """
    period = getattr(args, "period", None)
    limit_to_period = getattr(args, "limit_to_period", True)
    limit_session = getattr(args, "limit_session", None)
    out = []
    for session in config.sessions():
        if limit_to_period and period is not None and not session.startswith(f"{int(period):02d}"):
            continue
        if limit_session and not re.match(limit_session, session):
            continue
        out.append(session)
    return out


class Config(BaseConfig):
    # Media is the spine: a clip whose transcript has not arrived keeps empty
    # textContents (official text arrives later via --rebuild), and text
    # shows as non-empty textContents. A session is text-bearing when any
    # speech has text; align and NER skip the others.
    TEXT_SIGNAL = "textContents"
    TEXT_PROBE = "any"

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
            'speechkinds': data_dir / "original" / "media",
            'details': data_dir / "original" / "proceedings",
            'records': data_dir / "original" / "proceedings",
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

        Raw downloads land at ``original/media/{session}-ivods.json`` (IVOD
        index) and ``-speechkinds.json`` (dataset-221 speech kinds), and at
        ``original/proceedings/{session}-details.json`` (IVOD details with the
        WhisperX transcript) and ``-records.json`` (official per-clip text).
        The parsed files are ``original/media/{session}-media.json`` and
        ``original/proceedings/{session}-proceedings.json``.
        """
        suffix = '-ivods.json'
        return list(sorted(
            f.name[:-len(suffix)]
            for f in self.dir('media').glob(f'{prefix}*{suffix}')
        ))


if __name__ == '__main__':
    import sys
    config = Config(Path(sys.argv[1]))
    import IPython
    IPython.embed()
