"""WebVTT helpers: parse cues, merge HLS subtitle segments, time sentences.

A subtitle track timed against a clip gives sentence timings without forced
alignment. Cues are subtitle lines, not sentences: ``cues_to_sentences`` joins
the cue text, re-segments it with a caller-supplied sentencizer (the same one
the parliament's protocol text uses) and maps each sentence's character span
back to time by linear interpolation within the cues it covers.

HLS serves a subtitle rendition as short ``segment-N.vtt`` files; a cue that
straddles a segment boundary is repeated in both, so ``merge_segments``
de-duplicates by ``(id, start, end, text)``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from html import unescape
from typing import Callable, Iterable

_TS = r'(?:(?P<{p}h>\d+):)?(?P<{p}m>\d{{2}}):(?P<{p}s>\d{{2}})[.,](?P<{p}ms>\d{{3}})'
_TIMING_RE = re.compile(
    _TS.format(p="a") + r'\s+-->\s+' + _TS.format(p="b") + r'(?:\s+.*)?$')
_TAG_RE = re.compile(r'<[^>]+>')
_WS_RE = re.compile(r'\s+')


@dataclass(frozen=True)
class Cue:
    id: str
    start: float
    end: float
    text: str


def _seconds(m: re.Match, p: str) -> float:
    h = int(m.group(f"{p}h") or 0)
    return (h * 3600 + int(m.group(f"{p}m")) * 60 + int(m.group(f"{p}s"))
            + int(m.group(f"{p}ms")) / 1000)


def _clean(text: str) -> str:
    return _WS_RE.sub(" ", unescape(_TAG_RE.sub("", text))).strip()


def parse_vtt(text: str) -> list[Cue]:
    """Parse a WebVTT document into cues (inline tags stripped, lines joined).

    ``NOTE``/``STYLE``/``REGION`` blocks and cues with empty text are skipped.
    """
    cues: list[Cue] = []
    blocks = re.split(r'\r?\n\s*\r?\n', (text or "").lstrip("﻿"))
    for block in blocks:
        lines = [ln for ln in block.splitlines() if ln.strip()]
        if not lines or lines[0].startswith(("WEBVTT", "NOTE", "STYLE", "REGION")):
            continue
        cue_id = ""
        if "-->" not in lines[0]:
            cue_id, lines = lines[0].strip(), lines[1:]
        if not lines:
            continue
        m = _TIMING_RE.match(lines[0].strip())
        if not m:
            continue
        body = _clean(" ".join(lines[1:]))
        if body:
            cues.append(Cue(cue_id, _seconds(m, "a"), _seconds(m, "b"), body))
    return cues


def merge_segments(texts: Iterable[str]) -> list[Cue]:
    """Parse several WebVTT segments of one track into one cue list, dropping
    the cues repeated across segment boundaries, ordered by start time."""
    seen: set[tuple] = set()
    out: list[Cue] = []
    for text in texts:
        for cue in parse_vtt(text):
            key = (cue.id, cue.start, cue.end, cue.text)
            if key in seen:
                continue
            seen.add(key)
            out.append(cue)
    out.sort(key=lambda c: (c.start, c.end))
    return out


def _fmt(t: float) -> str:
    h, rem = divmod(max(t, 0.0), 3600)
    m, s = divmod(rem, 60)
    return f"{int(h):02d}:{int(m):02d}:{s:06.3f}"


def write_vtt(cues: Iterable[Cue]) -> str:
    """Serialise cues as a WebVTT document."""
    parts = ["WEBVTT", ""]
    for cue in cues:
        if cue.id:
            parts.append(cue.id)
        parts.append(f"{_fmt(cue.start)} --> {_fmt(cue.end)}")
        parts.append(cue.text)
        parts.append("")
    return "\n".join(parts)


def cues_to_sentences(cues: list[Cue], sentencize: Callable[[str], list[str]],
                      *, duration: float | None = None) -> list[dict]:
    """Re-segment cue text into sentences timed from the cues.

    The cue texts are joined with single spaces and passed to ``sentencize``.
    Each returned sentence is located in the joined text (in order) and its
    start/end characters are mapped to time within the cue that holds them,
    linearly by character position. Times are clamped to ``[0, duration]``
    (when given) and kept monotonic. Returns Stage 2 sentence dicts
    ``{"text", "timeStart", "timeEnd"}`` with times as ``"%.3f"`` strings.
    """
    if not cues:
        return []
    spans: list[tuple[int, int, float, float]] = []  # (char_from, char_to, t0, t1)
    pieces: list[str] = []
    pos = 0
    for cue in cues:
        if pieces:
            pos += 1  # joining space
        spans.append((pos, pos + len(cue.text), cue.start, cue.end))
        pieces.append(cue.text)
        pos += len(cue.text)
    full = " ".join(pieces)

    def time_at(c: int) -> float:
        for c0, c1, t0, t1 in spans:
            if c < c0:  # in the joining space before this cue
                return t0
            if c <= c1:
                return t0 + (t1 - t0) * (c - c0) / max(c1 - c0, 1)
        return spans[-1][3]

    out: list[dict] = []
    cursor = 0
    prev_start = 0.0
    hi = duration if duration is not None and duration > 0 else None
    for sent in sentencize(full):
        sent = sent.strip()
        if not sent:
            continue
        at = full.find(sent, cursor)
        if at < 0:
            at = full.find(sent[:20], cursor)
        if at < 0:
            at = cursor
            while at < len(full) and full[at] == " ":
                at += 1
        end = min(at + len(sent), len(full))
        cursor = end
        t0 = max(time_at(at), prev_start, 0.0)
        t1 = max(time_at(end), t0)
        if hi is not None:
            t0, t1 = min(t0, hi), min(t1, hi)
        prev_start = t0
        out.append({"text": sent, "timeStart": f"{t0:.3f}", "timeEnd": f"{t1:.3f}"})
    return out
