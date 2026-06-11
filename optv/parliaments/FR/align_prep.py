#! /usr/bin/env python3
"""Pre-slice per-séance FR HLS audio into per-speech MP3s for aeneas.

The Assemblée nationale publishes one HLS stream per séance (a multi-hour master
playlist). We download it once via ffmpeg → mono mp3 and re-encode each speech's
``[startOffset, startOffset + duration]`` slice. Séance pulls are long and flaky,
so this parliament enables the shared driver's truncation guard + resume
(``two_pass=True``, computing how far each séance must reach) with a generous
download timeout.

The shared driver in :mod:`optv.shared.audio_prep` owns the download-once / slice
/ cache machinery; this module only supplies the FR field mapping.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg, md5_key, slice_reencode,
    prepare_per_speech_audio as _prepare,
)

# Séances run 10–12 h; allow a long single-stream pull.
_DOWNLOAD_TIMEOUT = 10800


def _extract(speech: dict) -> Optional[SpeechAudio]:
    media = speech.get("media") or {}
    audio_url = media.get("audioFileURI")
    addinfo = media.get("additionalInformation") or {}
    start_offset = addinfo.get("startOffset")
    duration = media.get("duration")
    if not audio_url or start_offset is None or duration is None:
        if duration == 0:
            speech.setdefault("debug", {})["align-skip"] = "zero-duration-from-source"
        return None
    if float(duration) < 0.1:
        speech.setdefault("debug", {})["align-skip"] = "sub-100ms-duration"
        return None
    key = addinfo.get("crvId") or media.get("originMediaID") or md5_key(audio_url)
    return SpeechAudio(source_url=audio_url, start=float(start_offset),
                       duration=float(duration), session_key=key)


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    download_ffmpeg(url, target, required_duration=required_duration, hls=True,
                    timeout=_DOWNLOAD_TIMEOUT)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force, two_pass=True,
                    extract=_extract, download_session=_download, slice_fn=slice_reencode)
