"""Attach proceedings text onto a fixed media spine (the PT/ES pattern).

The media spine is the record source and never moves; this only fills
``textContents`` (and ``debug.proceedingIndex``) on the speeches whose surname
key matches a proceedings turn, via the shared Needleman-Wunsch aligner. Used by
every PDF-tier merger so the join lives in one place.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from ..sequence_align import align_equal_keys

logger = logging.getLogger(__name__)


def load_turns(config, session: str) -> list[dict]:
    """Return the parsed proceedings turns for a session, or [] if none."""
    pf = config.file(session, "proceedings")
    if not pf.exists():
        return []
    try:
        return json.loads(pf.read_text()).get("data") or []
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"[{session}] could not read proceedings: {e}")
        return []


def join_text_to_spine(merged: list[dict], spine_keys: list[str], turns: list[dict],
                       *, creator: str, license: str, language: str = "de") -> int:
    """Surname NW join; fill ``textContents`` + ``debug.proceedingIndex`` on
    matched spine records. ``spine_keys[i]`` is the match key for ``merged[i]``.
    Returns the number of matched speeches. The spine order/identity is untouched.
    """
    if not turns:
        return 0
    text_keys = [t.get("matchKey") or "" for t in turns]
    mapping = dict(align_equal_keys(spine_keys, text_keys))
    for si, ti in mapping.items():
        turn = turns[ti]
        sentences = turn.get("sentences") or []
        if not sentences:
            continue
        rec = merged[si]
        speaker = rec["people"][0]["label"] if rec.get("people") else ""
        rec["textContents"] = [{
            "type": "proceedings",
            "language": language,
            "creator": creator,
            "license": license,
            "textBody": [{
                "type": "speech",
                "speaker": speaker,
                "sentences": sentences,
            }],
        }]
        rec.setdefault("debug", {})["proceedingIndex"] = turn.get("index")
        rec["debug"]["proceedingTextID"] = turn.get("originTextID")
    return len(mapping)


def attach_text_by_index(merged: list[dict], turns: list[dict],
                         *, creator: str, license: str, language: str = "de") -> int:
    """Attach text to spine records by ``speechIndex`` (1:1, no alignment needed).

    Used when the text source is already aligned to the spine — e.g. DE-NI, whose
    broadcaster WebVTT is sliced onto each clip by time, so each turn already
    carries the spine ``speechIndex`` it belongs to. Returns the match count.
    """
    by_index = {t.get("speechIndex"): t for t in turns if t.get("speechIndex") is not None}
    matched = 0
    for rec in merged:
        turn = by_index.get(rec.get("speechIndex"))
        if not turn:
            continue
        sentences = turn.get("sentences") or []
        if not sentences:
            continue
        speaker = rec["people"][0]["label"] if rec.get("people") else ""
        rec["textContents"] = [{
            "type": "proceedings",
            "language": language,
            "creator": creator,
            "license": license,
            "textBody": [{"type": "speech", "speaker": speaker, "sentences": sentences}],
        }]
        rec.setdefault("debug", {})["proceedingIndex"] = turn.get("speechIndex")
        matched += 1
    return matched
