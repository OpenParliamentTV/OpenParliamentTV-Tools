"""Normalize AT Mediathek agenda titles into the two-title model.

The Mediathek ``debatte.content`` bundles a TOP number and a descriptive label
in one string (e.g. ``"TOP 7 Nächtliche Dauerbeleuchtung von Windrädern"``).
The platform's agendaItem carries two fields:

- ``officialTitle`` — the formal, normalized agenda label
  (``"Tagesordnungspunkt 7"`` / ``"Tagesordnungspunkte 1 bis 3"``)
- ``title`` — the human-readable subject (``"Nächtliche Dauerbeleuchtung …"``)

This mirrors the DE ``fix_title`` split. Only ``TOP N …`` items split cleanly;
everything else (``Abstimmung über …``, ``Fragestunde …``, ``Aktuelle Stunde
…``) has no number/subject boundary, so both fields keep the full string.

The agenda *type* is classified separately on the raw title via
``optv.shared.agenda_types.classify_at`` (so ``"TOP 1 Budgetrede …"`` still
classifies as budget even after the number is split off).
"""

from __future__ import annotations

import re

# "TOP 7 <subject>", "TOP 1-3 <subject>", "TOP 16–19 <subject>" (hyphen/en-dash
# range), and comma lists "TOP 8-9,10 <subject>". Group 1 is the number spec; the
# subject (group 2) is required — a bare "TOP 5" with no label falls through.
_TOP_RE = re.compile(r"^TOP\s+(\d+(?:\s*[-–,]\s*\d+)*)\s+(\S.*)$", re.I)

# Recurring source typo for the non-TOP branch ("Tageordnungspunkte").
_TYPO_RE = re.compile(r"\bTageordnungspunkt", re.I)


def _format_official(numspec: str) -> str:
    """Turn a TOP number spec into a normalized official label.

    ``"5"`` → ``"Tagesordnungspunkt 5"``; ``"1-3"`` → ``"Tagesordnungspunkte 1
    bis 3"``; ``"8-9,10"`` → ``"Tagesordnungspunkte 8 bis 9, 10"``.
    """
    if re.fullmatch(r"\d+", numspec):
        return f"Tagesordnungspunkt {numspec}"
    pretty = re.sub(r"\s*[-–]\s*", " bis ", numspec)
    pretty = re.sub(r"\s*,\s*", ", ", pretty)
    return f"Tagesordnungspunkte {pretty}"


def split_agenda_title(raw: str) -> tuple[str, str]:
    """Return ``(officialTitle, title)`` for a raw Mediathek agenda string.

    ``TOP N …`` items split into a normalized official label plus the
    descriptive subject. Any other title is returned unchanged in both slots
    (after fixing the ``Tageordnungspunkte`` typo).
    """
    raw = (raw or "").strip()
    raw = _TYPO_RE.sub("Tagesordnungspunkt", raw)
    m = _TOP_RE.match(raw)
    if not m:
        return raw, raw
    return _format_official(m.group(1)), m.group(2).strip()
