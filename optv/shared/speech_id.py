#! /usr/bin/env python3
"""Speech-level id model normalization.

The Stage-2 id model has three distinct slots, each at its own level:

- ``speech.originID``               — a **joint** speech id, set **only** when the
  source provides one identity spanning media ⋈ proceedings (e.g. SE's
  ``anforande``-based key). Absent otherwise.
- ``media.originMediaID``           — the media source id.
- ``textContents[].originTextID``   — the proceedings / text source id.

Historically several mergers also wrote the text id (or, for DE-RP, the media
id) into the top-level ``originID`` / ``originTextID``, duplicating an id that
already lives at its own level. ``normalize_speech_originid`` is called at the
**end** of a merger (after any internal use of ``originID``) to enforce the
model: promote the legacy speech-level ``originTextID`` to ``originID``, then
drop ``originID`` when it merely repeats the media or text id. A genuine joint
id (distinct from both) is kept.
"""

from __future__ import annotations


def normalize_speech_originid(speech: dict) -> None:
    """Enforce the speech-id model on one speech dict, in place."""
    if not isinstance(speech, dict):
        return
    # Promote the legacy speech-level alias `originTextID` to `originID`.
    if "originTextID" in speech:
        legacy = speech.pop("originTextID")
        speech.setdefault("originID", legacy)

    oid = speech.get("originID")
    if not oid:                                  # empty / missing → not a joint id
        speech.pop("originID", None)
        return

    media_id = (speech.get("media") or {}).get("originMediaID")
    text_ids = {
        tc.get("originTextID")
        for tc in (speech.get("textContents") or [])
        if isinstance(tc, dict)
    }
    # Redundant with the media id or any text id → it is not a joint id; drop it.
    if oid == media_id or oid in text_ids:
        del speech["originID"]
