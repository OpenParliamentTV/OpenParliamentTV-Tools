"""Sitting clock times from a German plenary protocol.

German Landtag protocols state the sitting's clock times to the minute: the
start (``Beginn: 9:33 Uhr``), the end (``Schluss: 16:44 Uhr``), and every
interruption in the body (``(Unterbrechung der Sitzung: 12:04 Uhr)`` …
``(Wiederaufnahme der Sitzung: 13:16 Uhr)``; Sachsen-Anhalt writes
``Unterbrechung: 10:14 Uhr.`` … ``Wiederbeginn: 11:15 Uhr.`` and full hours
without minutes, ``Beginn: 10 Uhr``). Some cover pages also summarise the lunch
break (``Mittagspause: 12:04 bis 13:16 Uhr``).

:func:`parse_sitting_times` reads these from plain text into
``{"begin": "HH:MM", "end": "HH:MM", "breaks": [["HH:MM", "HH:MM"], …]}``;
:func:`extract_sitting_times` runs it over a PDF's text.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from .extract_text import extract_blocks

logger = logging.getLogger(__name__)

_T = r"(\d{1,2})(?:[:.](\d{2}))?\s*Uhr"
_BEGIN_RE = re.compile(r"\bBeginn:\s*" + _T)
_END_RE = re.compile(r"\bSchluss:\s*" + _T)
_BREAK_RE = re.compile(r"\bUnterbrechung(?:\s+der\s+Sitzung)?:\s*" + _T)
_RESUME_RE = re.compile(
    r"(?:\bWiederauf-?\s*nahme\s+der\s+Sitzung|\bWiederbeginn):\s*" + _T)
_LUNCH_RE = re.compile(
    r"Mittagspause:\s*(\d{1,2})[:.](\d{2})\s*bis\s*(\d{1,2})[:.](\d{2})\s*Uhr")


def _hm(h: str, m: Optional[str]) -> Optional[str]:
    hh, mm = int(h), int(m or 0)
    if hh > 23 or mm > 59:
        return None
    return f"{hh:02d}:{mm:02d}"


def _times(regex: re.Pattern, text: str) -> list[str]:
    return [t for m in regex.finditer(text) if (t := _hm(m.group(1), m.group(2)))]


def parse_sitting_times(text: str) -> Optional[dict]:
    """Return the sitting's start, end and breaks from protocol text.

    ``begin`` is the first ``Beginn:`` time and ``end`` the last ``Schluss:``
    time. Breaks pair each interruption with the next resumption at or after it,
    in time order (two-column page text does not keep the reading order
    reliably); a cover-page ``Mittagspause`` adds its pair. Returns ``None``
    when the start or end is missing or the end is not after the start.
    """
    text = re.sub(r"\s+", " ", text or "")
    begins, ends = _times(_BEGIN_RE, text), _times(_END_RE, text)
    if not begins or not ends:
        return None
    begin, end = begins[0], ends[-1]
    if end <= begin:
        return None

    breaks: set[tuple[str, str]] = set()
    resumes = sorted(set(_times(_RESUME_RE, text)))
    for stop in sorted(set(_times(_BREAK_RE, text))):
        nxt = next((r for r in resumes if r >= stop), None)
        if nxt is not None:
            breaks.add((stop, nxt))
    for m in _LUNCH_RE.finditer(text):
        a, b = _hm(m.group(1), m.group(2)), _hm(m.group(3), m.group(4))
        if a and b and a <= b:
            breaks.add((a, b))
    return {
        "begin": begin,
        "end": end,
        "breaks": [list(b) for b in sorted(breaks) if begin <= b[0] and b[1] <= end],
    }


def extract_sitting_times(pdf_path: Path) -> Optional[dict]:
    """:func:`parse_sitting_times` over the text of every page of ``pdf_path``."""
    try:
        blocks = extract_blocks(Path(pdf_path))
    except Exception as e:  # unreadable PDF: no times, the merge falls back
        logger.warning(f"{pdf_path}: could not read sitting times ({e})")
        return None
    return parse_sitting_times(" ".join(b.get("text", "") for b in blocks))
