#! /usr/bin/env python3
"""Pre-slice per-meeting Storting audio into per-speech MP3s for aeneas.

Stortinget publishes one MP4 per "del" (part) of a meeting, each 1–6 hours long.
We extract its audio once (ffmpeg mp4 → mp3) and stream-copy each speech's
``[startOffset, startOffset + duration]`` out of it. The source MP4 URL and the
per-part ``qbvid`` live in ``media.additionalInformation``; the shared driver in
:mod:`optv.shared.audio_prep` owns the download-once / slice / cache machinery.

NO additionally writes the per-speech mp3 path back onto ``media.audioFileURI``
(the merger leaves it absent until the slice exists) so the platform's
downstream tooling can locate it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg, md5_key, slice_copy,
    prepare_per_speech_audio as _prepare,
)


def _extract(speech: dict) -> Optional[SpeechAudio]:
    media = speech.get("media") or {}
    addinfo = media.get("additionalInformation") or {}
    # Prefer the low-bitrate audio source URL written by the merger; fall back to
    # the high-bitrate platform URL for merged files predating that field.
    mp4_url = addinfo.get("audio_source_url") or addinfo.get("mp4_url")
    start_offset = addinfo.get("startOffset")
    duration = media.get("duration")
    if not mp4_url or start_offset is None or not duration:
        return None
    key = addinfo.get("qbvid") or md5_key(mp4_url)
    return SpeechAudio(source_url=mp4_url, start=float(start_offset),
                       duration=float(duration), session_key=key)


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    download_ffmpeg(url, target, required_duration=required_duration, hls=False)


def _writeback(speech: dict, target: Path) -> None:
    speech.setdefault("media", {})["audioFileURI"] = str(target)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=_download, slice_fn=slice_copy,
                    on_prepared=_writeback, on_existing=_writeback)
