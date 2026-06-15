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

# Confidence gate for the text↔spine join, ported from the DE merger's pattern
# (optv/parliaments/DE/merger/merge_session.py). The surname Needleman-Wunsch
# mis-attributes text on Q&A-structured items and on gross length mismatches; we
# can't always *fix* that join, but we can *flag* it so the platform import gate
# (confidence == 1) drops the unreliable text — "wrong text is worse than none".
# Only signals that survive without an ASR oracle: the agenda type, and the
# clip's chars-per-second (text physically too long for the clip = mis-merge).
QA_TYPES = frozenset({"qa", "questioning_of_the_government"})
CPS_CAP = 100.0          # chars/sec above this ⇒ text can't fit the clip
CPS_CAP_CHARS_FLOOR = 500  # don't gate short speeches on cps alone


def _text_char_count(text_contents: list) -> int:
    return sum(len(s.get("text", ""))
               for c in (text_contents or [])
               for tb in (c.get("textBody") or [])
               for s in (tb.get("sentences") or []))


def assign_join_confidence(merged: list[dict]) -> int:
    """Stamp ``debug.confidence`` (+ ``confidence_reason``) on text-bearing spine
    speeches so the platform can drop unreliable joins. Clean matches get 1.0;
    Q&A agenda types and cps-cap mis-merges get 0.5. Speeches with no text are
    left untouched (video-only, nothing to gate). Returns the count gated to 0.5.
    """
    gated = 0
    for sp in merged:
        if not sp.get("textContents"):
            continue
        agenda_type = (sp.get("agendaItem") or {}).get("type") or ""
        conf, reason = 1.0, None
        if agenda_type in QA_TYPES:
            conf, reason = 0.5, "qa-agenda-type"
        else:
            ai = (sp.get("media") or {}).get("additionalInformation") or {}
            start, end = ai.get("startOffset"), ai.get("endOffset")
            if start is not None and end is not None and end > start:
                chars = _text_char_count(sp["textContents"])
                if chars >= CPS_CAP_CHARS_FLOOR and chars / (end - start) >= CPS_CAP:
                    conf, reason = 0.5, "cps-cap"
        dbg = sp.setdefault("debug", {})
        dbg["confidence"] = conf
        if reason:
            dbg["confidenceReason"] = reason
            gated += 1
        else:
            dbg.pop("confidenceReason", None)
    return gated


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


def _text_body(turn: dict, speaker: str, sentences: list) -> list[dict]:
    """Ordered textBody for a matched turn: the proceedings turn's typed
    speech/comment bodies (DE structure — comments kept for display, aligned only
    if speech), or a single speech body when the turn carries no ``bodies``."""
    bodies = turn.get("bodies")
    if not bodies:
        return [{"type": "speech", "speaker": speaker, "sentences": sentences}]
    out = []
    for b in bodies:
        if b.get("type") == "comment":
            out.append({"type": "comment", "speaker": None,
                        "sentences": b.get("sentences", [])})
        else:
            out.append({"type": "speech", "speaker": speaker,
                        "sentences": b.get("sentences", [])})
    return out


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
            "textBody": _text_body(turn, speaker, sentences),
        }]
        rec.setdefault("debug", {})["proceedingIndex"] = turn.get("index")
        rec["debug"]["proceedingTextID"] = turn.get("originTextID")
    assign_join_confidence(merged)
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
            "textBody": _text_body(turn, speaker, sentences),
        }]
        rec.setdefault("debug", {})["proceedingIndex"] = turn.get("speechIndex")
        matched += 1
    assign_join_confidence(merged)
    return matched
