#! /usr/bin/env python3
"""Pre-slice per-session PT HLS audio into per-speech MP3s for aeneas.

av.parlamento.pt serves one continuous HLS stream per reunião (the un-clipped
``…/{session}.mp4/index.m3u8``). We download it once via ffmpeg → mono mp3 and
re-encode each speech's ``[startOffset, startOffset + duration]`` slice. The av
media has no per-session id, so the session is keyed by a hash of its URL.

The shared driver in :mod:`optv.shared.audio_prep` owns the download-once / slice
/ cache machinery; this module only supplies the PT field mapping. Only speeches
that actually carry text are staged.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg, md5_key, slice_reencode,
    prepare_per_speech_audio as _prepare,
)

# av.parlamento.pt reuniãos can run several hours.
_DOWNLOAD_TIMEOUT = 7200


def _extract(speech: dict) -> Optional[SpeechAudio]:
    # Only speeches that actually carry text need aligning.
    tcs = speech.get("textContents") or []
    if not any((tc.get("textBody") or []) for tc in tcs):
        return None

    media = speech.get("media") or {}
    audio_url = media.get("audioFileURI")
    addinfo = media.get("additionalInformation") or {}
    start_offset = addinfo.get("startOffset")
    duration = media.get("duration")
    if not audio_url or start_offset is None or not duration:
        if duration == 0:
            speech.setdefault("debug", {})["align-skip"] = "zero-duration-from-source"
        return None
    return SpeechAudio(source_url=audio_url, start=float(start_offset),
                       duration=float(duration), session_key=md5_key(audio_url))


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    download_ffmpeg(url, target, required_duration=required_duration, hls=True,
                    timeout=_DOWNLOAD_TIMEOUT)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech with text has a per-speech MP3 ready for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=_download, slice_fn=slice_reencode)
