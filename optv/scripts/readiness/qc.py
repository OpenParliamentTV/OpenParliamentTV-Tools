"""Bring the Whisper QC figures of a parliament's sample sessions up to date.

Comparing a session's text with its transcript takes about half a second per speech, too long for ``check``. The figures are therefore cached per session in ``<data_dir>/cache/whisper/<session>-qc.json`` and recomputed here when the text or the transcript changed.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .scorecard import build_context

logger = logging.getLogger(__name__)

CURRENT = "current"
WRITTEN = "written"
NO_TRANSCRIPT = "no transcript"
NOT_PUBLISHED = "not published"
NO_TEXT = "no text expected"


def refresh(code: str, *, sessions: list[str] | None = None, force: bool = False,
            data_dir: Path | None = None) -> dict[str, str]:
    """Recompute the outdated QC figures of ``code``. Returns what happened per sample session."""
    from optv.shared.whisper_diff import whisper_path, write_session_qc

    ctx = build_context(code, data_dir=data_dir)
    if ctx.config is None:
        raise FileNotFoundError(f"no data repository for {code} on this machine")
    data_dir = ctx.config.dir("data")
    evidence = {q.sample_id: q for q in ctx.qc}
    outcome = {}
    for sample in ctx.samples:
        sid = sample.sample.id
        if sessions and sid not in sessions:
            continue
        if sample.doc is None:
            outcome[sid] = NOT_PUBLISHED
        elif not sample.sample.expect_text:
            outcome[sid] = NO_TEXT
        elif not whisper_path(data_dir, sid).is_file():
            outcome[sid] = NO_TRANSCRIPT
        elif not force and sid in evidence and evidence[sid].rows is not None and evidence[sid].current:
            outcome[sid] = CURRENT
        else:
            logger.info(f"{code} {sid}: comparing text and transcript")
            write_session_qc(data_dir, sid)
            outcome[sid] = WRITTEN
    return outcome
