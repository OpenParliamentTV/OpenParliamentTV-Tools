"""Media↔proceedings link confidence — the platform text-import gate.

The platform imports a speech's *text* only when ``debug.confidence == 1`` and
``len(debug.linkedMediaIndexes) == 1`` (see ``api/v1/modules/media.php``); a
media clip always imports regardless. So lowering confidence below 1 removes
suspect text while keeping the video.

This helper centralises the agenda-type / chars-per-second heuristics the DE
merger pioneered (``optv/parliaments/DE/merger/merge_session.py``) so other
parliaments can reuse them without copying the logic. It is intentionally
parameterised: each caller chooses which types to blanket-suppress and the
chars-per-second floor/cap appropriate to its source.

A speech whose proceedings text is far longer than its media clip could
physically contain (``chars / duration`` well above human speaking rate) is a
mis-merge: a whole-debate or wrong-text block bound onto a short clip. German
speech runs ~16 cps; mis-merges run 100s. ``cps_cap`` gates those.
"""

from __future__ import annotations

from typing import Optional


# Substantive *debate* types where extreme chars/sec means wrong text. Procedural
# / opening / voting / election etc. legitimately carry long chair text
# (announcements, referral lists) on a short representative clip — correct-but-
# truncated, not wrong — so they are deliberately excluded. Mirrors the DE set.
DEFAULT_CPS_CAP_TYPES = frozenset({
    'regular', 'report', 'current_affairs', 'government_declaration',
    'budget', 'briefing', 'questioning_of_the_government',
})

DEFAULT_CPS_CAP = 100


def compute_confidence(core_type: Optional[str], chars: int,
                       duration: Optional[float], *,
                       blanket_types: frozenset = frozenset(),
                       cps_cap_types: frozenset = DEFAULT_CPS_CAP_TYPES,
                       cps_cap: float = DEFAULT_CPS_CAP,
                       cps_floor: int = 25000) -> tuple[float, Optional[str]]:
    """Return ``(confidence, reason)`` for one merged speech.

    ``confidence`` starts at 1.0 and is capped to 0.5 when:

    - ``core_type`` is in ``blanket_types`` (a type known to be unreliable
      regardless of length), or
    - ``core_type`` is in ``cps_cap_types`` and the text is both long
      (``chars >= cps_floor``) and physically too dense for the clip
      (``chars / duration >= cps_cap``).

    ``reason`` is ``None`` when confidence stays 1.0, else ``'blanket-type'`` or
    ``'cps-cap'`` (blanket wins when both apply).
    """
    confidence = 1.0
    reason: Optional[str] = None
    if core_type in blanket_types:
        confidence = 0.5
        reason = 'blanket-type'
    if (core_type in cps_cap_types
            and isinstance(duration, (int, float)) and duration > 0
            and chars >= cps_floor and chars / duration >= cps_cap):
        confidence = min(confidence, 0.5)
        reason = reason or 'cps-cap'
    return confidence, reason
