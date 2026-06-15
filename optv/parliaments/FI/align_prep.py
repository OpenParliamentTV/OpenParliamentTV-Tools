#! /usr/bin/env python3
"""Pre-slice per-session FI HLS audio into per-speech MP3s for aeneas.

Eduskunta publishes one HLS stream per plenary session. We download it once
(ffmpeg follows the m3u8 manifest, ``-vn`` strips video) and re-encode each
speech's ``[startOffset, startOffset + duration]`` slice for decoder-exact
timestamps. ``startOffset`` (the broadcast ``time``) and ``duration`` are written
by the merger into ``media``; the shared driver in :mod:`optv.shared.audio_prep`
owns the download-once / slice / cache machinery.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg, md5_key, slice_reencode,
    prepare_per_speech_audio as _prepare,
)


def _extract(speech: dict) -> Optional[SpeechAudio]:
    media = speech.get("media") or {}
    audio_url = media.get("audioFileURI")
    addinfo = media.get("additionalInformation") or {}
    start_offset = addinfo.get("startOffset")
    duration = media.get("duration")
    if not audio_url or start_offset is None or not duration:
        if duration == 0:
            speech.setdefault("debug", {})["alignSkip"] = "zero-duration-from-source"
        return None
    key = addinfo.get("eventRef") or md5_key(audio_url)
    return SpeechAudio(source_url=audio_url, start=float(start_offset),
                       duration=float(duration), session_key=key)


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    download_ffmpeg(url, target, required_duration=required_duration, hls=True)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=_download, slice_fn=slice_reencode)
