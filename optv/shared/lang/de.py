#! /usr/bin/env python3
"""German parliamentary-text conventions.

Shared by every German parliament (DE, DE-RP, DE-ST and the video-only
Landtage): academic-honorific stripping and chair-title → speaker ``context``
mapping. Add per-parliament deltas in that parliament's own merger/parser;
anything common to *German* belongs here.
"""

from __future__ import annotations

# German academic honorifics stripped from speaker names before first/last split.
HONORIFICS = ("Dr. ", "Prof. ", "Prof. Dr. ", "Dr. Dr. ", "Dr. h. c. ")


def strip_honorifics(name: str, honorifics: tuple[str, ...] = HONORIFICS) -> str:
    """Repeatedly strip leading academic honorifics (``Dr. ``, ``Prof. ``…)."""
    s = name.strip()
    changed = True
    while changed:
        changed = False
        for h in honorifics:
            if s.startswith(h):
                s = s[len(h):]
                changed = True
                break
    return s


def speaker_context(role: str) -> str:
    """Map a German chair/role string to a Stage-2 speaker ``context``.

    Vice-president variants (``Vizepräsident``, ``stellv. Präsident``) →
    ``vice-president``; president variants → ``president``; everything else
    (MPs, government members) → ``main-speaker``.
    """
    r = (role or "").lower()
    if ("vizepräsident" in r or "vizepraesident" in r
            or ("stellv" in r and ("präsident" in r or "praesident" in r))):
        return "vice-president"
    if "präsident" in r or "praesident" in r:
        return "president"
    return "main-speaker"
