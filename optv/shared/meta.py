"""Canonical Stage-2 ``meta`` block builder.

Every parliament's merger used to hand-assemble its own ``meta`` dict, which
drifted: ``schemaVersion`` was sometimes absent, ``electoralPeriod`` was a bare
int in some parliaments and ``{"number": N}`` in others, and ``parliament`` /
``lastProcessing`` / ``lastUpdate`` were inconsistently present. ``build_meta``
emits one canonical, ordered block so the output is uniform across parliaments.

Canonical key order::

    schemaVersion, parliament, electoralPeriod, session,
    dateStart, dateEnd, lastProcessing, lastUpdate, processing

``electoralPeriod`` is always normalised to ``{"number": int}`` (the shape the
per-speech records and the Conductor reader expect). ``processing`` /
``lastProcessing`` / ``lastUpdate`` mirror what ``optv.shared.audio_prep`` stamps
at the align stage, so a later stage that re-stamps them stays consistent.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Union

SCHEMA_VERSION = "1.0"


def fill_original_language(speeches: Iterable[dict], parliament_id: str) -> None:
    """Set ``speech.originalLanguage`` from the manifest ``language_code`` for
    every speech that doesn't already carry one.

    Monolingual parliaments get the manifest default; multilingual ones (EU, FI)
    set their own per-speech value upstream and are left untouched by the
    ``setdefault`` semantics. Mutates the speeches in place.
    """
    from optv.parliaments import get_language

    lang = get_language(parliament_id)
    if not lang:
        return
    for speech in speeches:
        if not speech.get("originalLanguage"):
            speech["originalLanguage"] = lang

_CANONICAL_ORDER = (
    "schemaVersion",
    "session",
    "dateStart",
    "dateEnd",
    "lastProcessing",
    "lastUpdate",
    "processing",
)

# Never emitted at meta level: they duplicate the per-speech fields and the
# platform keys off the per-item values. Dropped from inherited/extra too.
_NEVER_IN_META = ("parliament", "electoralPeriod")


def now_iso() -> str:
    """Processing timestamp in the Stage-2 datetime format (UTC, seconds)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_electoral_period(period: Union[int, dict, None]) -> Optional[dict]:
    """Coerce an electoral period to the canonical ``{"number": int}`` shape."""
    if period is None:
        return None
    if isinstance(period, dict):
        if period.get("number") is None:
            return None
        return {**period, "number": int(period["number"])}
    return {"number": int(period)}


def build_meta(
    parliament_id: str,
    *,
    session: str,
    processing: dict,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    electoral_period: Union[int, dict, None] = None,
    last_processing: str = "merge",
    last_update: Optional[str] = None,
    inherit: Optional[dict] = None,
    extra: Optional[dict] = None,
) -> dict[str, Any]:
    """Build a canonical Stage-2 ``meta`` block.

    ``meta`` intentionally does **not** carry ``parliament`` or
    ``electoralPeriod`` — those live on every speech (``data[].parliament`` /
    ``data[].electoralPeriod``) and the platform keys off the per-item values, so
    a meta-level copy is pure duplication. ``parliament_id`` / ``electoral_period``
    remain in the signature only because callers pass them (and a future need may
    arise); they are not emitted.

    ``inherit`` carries forward keys from an upstream meta block (e.g. the DE
    proceedings meta) that the canonical keys then override. ``extra`` holds
    parliament-specific keys (``sourceURI``, ``sourceLabel``, …) appended after
    the canonical ones. ``None`` values are dropped.
    """
    merged: dict[str, Any] = {}
    if inherit:
        merged.update(inherit)

    canonical = {
        "schemaVersion": SCHEMA_VERSION,
        "session": session,
        "dateStart": date_start if date_start is not None else merged.get("dateStart"),
        "dateEnd": date_end if date_end is not None else merged.get("dateEnd"),
        "lastProcessing": last_processing,
        "lastUpdate": last_update or now_iso(),
        "processing": processing,
    }
    for key, value in canonical.items():
        if value is not None:
            merged[key] = value

    if extra:
        for key, value in extra.items():
            if value is not None:
                merged[key] = value

    for key in _NEVER_IN_META:
        merged.pop(key, None)

    ordered = {k: merged[k] for k in _CANONICAL_ORDER if k in merged}
    for key, value in merged.items():
        if key not in ordered:
            ordered[key] = value
    return ordered
