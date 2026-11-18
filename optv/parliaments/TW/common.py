#! /usr/bin/env python3
"""Per-parliament config + on-disk layout for Taiwan's Legislative Yuan (TW).

Session keys are ``{屆:02d}{會期:02d}{會次:03d}`` strings (e.g. ``"1105011"`` =
term 11, session-period 5, plenary meeting 11 → meeting code ``院會-11-5-11``).
The fixed-width prefix is what makes ``--period=11`` work via the shared
``session.startswith(str(args.period))`` filter.
"""

import re

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


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
    # Sessions are discovered from the raw IVOD index downloads. Raw downloads
    # land at ``original/media/{session}-ivods.json`` (IVOD index) and
    # ``-speechkinds.json`` (dataset-221 speech kinds), and at
    # ``original/proceedings/{session}-details.json`` (IVOD details with the
    # WhisperX transcript) and ``-records.json`` (official per-clip text). The
    # parsed files are ``original/media/{session}-media.json`` and
    # ``original/proceedings/{session}-proceedings.json``.
    MEDIA_GLOB_SUFFIX = "-ivods.json"
    # The raw downloads share the media and proceedings directories and differ
    # by filename suffix, so each gets a stage name of its own.
    EXTRA_DIRS = {
        "ivods": ("data", "original/media"),
        "speechkinds": ("data", "original/media"),
        "details": ("data", "original/proceedings"),
        "records": ("data", "original/proceedings"),
    }
