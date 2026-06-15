#! /usr/bin/env python3
"""Pre-slice per-debate Riksdag audio into per-speech MP3s for aeneas.

Riksdag publishes one MP3 per debate (~40 min, 5–40 speeches). The source is
already an mp3 file (plain HTTP, not HLS), so we download it once and stream-copy
each speech's ``[startOffset, startOffset + duration]`` out of it. The shared
driver in :mod:`optv.shared.audio_prep` owns the download-once / slice / cache
machinery; this module only supplies the SE-specific field mapping.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from optv.shared.audio_prep import (
    SpeechAudio, download_http, md5_key, slice_copy,
    prepare_per_speech_audio as _prepare,
)


def _extract(speech: dict) -> Optional[SpeechAudio]:
    media = speech.get("media") or {}
    audio_url = media.get("audioFileURI")
    addinfo = media.get("additionalInformation") or {}
    start_offset = addinfo.get("startOffset")
    duration = media.get("duration")
    if not audio_url or start_offset is None or not duration:
        # Procedural interventions (talman calls) come through with
        # anf_sekunder=0 despite carrying text — slicing 0s yields an empty mp3.
        if duration == 0:
            speech.setdefault("debug", {})["alignSkip"] = "zero-duration-from-source"
        return None
    key = media.get("originMediaID") or md5_key(audio_url)
    return SpeechAudio(source_url=audio_url, start=float(start_offset),
                       duration=float(duration), session_key=key)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=download_http, slice_fn=slice_copy)
