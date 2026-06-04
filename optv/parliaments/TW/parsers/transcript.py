"""Helpers for working with ly.govapi.tw transcript blobs.

The ``transcript.whisperx`` field is a list of segments with absolute
``start`` / ``end`` seconds (offset into the IVOD clip) and ``text``. Each
segment is roughly one spoken phrase. We treat each segment as one Stage 2
sentence — the boundaries are already where Whisper aligned them and they
land at speakable pauses, which is exactly what the platform wants for
karaoke-style highlighting.

Whisperx timings are floats; Stage 2 sentences require timeStart/timeEnd as
strings matching ``^\d+(\.\d+)?$``, so we format with three decimals.
"""

from __future__ import annotations

from typing import Iterable

# How many decimals to keep on whisperx timings when serialising.
_TIMING_DECIMALS = 3


def _fmt(seconds: float | int | None) -> str | None:
    if seconds is None:
        return None
    try:
        return f"{max(0.0, float(seconds)):.{_TIMING_DECIMALS}f}"
    except (TypeError, ValueError):
        return None


def whisperx_to_sentences(segments: Iterable[dict] | None) -> list[dict]:
    """Convert whisperx segments into Stage 2 ``sentences[]``.

    Empty / missing text is dropped (the platform's renderer doesn't have a
    use for blank sentences and the schema's sentence validator complains).
    """
    out: list[dict] = []
    for seg in segments or []:
        text = (seg.get("text") or "").strip()
        if not text:
            continue
        sentence = {"text": text}
        ts = _fmt(seg.get("start"))
        te = _fmt(seg.get("end"))
        if ts is not None:
            sentence["timeStart"] = ts
        if te is not None:
            sentence["timeEnd"] = te
        out.append(sentence)
    return out


def whisperx_max_time(segments: Iterable[dict] | None) -> float:
    """Return the latest ``end`` timestamp across all segments (0 if empty).

    Used to populate ``debug.align-duration`` and to sanity-check against
    media.duration.
    """
    latest = 0.0
    for seg in segments or []:
        try:
            v = float(seg.get("end") or 0)
        except (TypeError, ValueError):
            continue
        if v > latest:
            latest = v
    return latest


def gazette_paragraphs(gazette: dict | None) -> list[str]:
    """Pull non-empty paragraph text out of a gazette transcript block.

    The ly.govapi.tw ``gazette`` field is sparse: present on some older
    IVODs, ``None`` on most recent term 11 ones. The shape varies; we
    expose a tolerant extractor used as a fall-through behind whisperx.
    """
    if not isinstance(gazette, dict):
        return []
    # Common shapes seen: gazette["paragraphs"] (list[str]),
    # gazette["content"] (str with newlines), gazette["text"] (str).
    paragraphs: list[str] = []
    if isinstance(gazette.get("paragraphs"), list):
        paragraphs.extend(
            str(p).strip() for p in gazette["paragraphs"] if str(p).strip()
        )
    for key in ("content", "text", "body"):
        v = gazette.get(key)
        if isinstance(v, str) and v.strip():
            paragraphs.extend(line.strip() for line in v.splitlines() if line.strip())
    return paragraphs
