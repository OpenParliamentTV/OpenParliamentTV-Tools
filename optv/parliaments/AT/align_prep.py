#! /usr/bin/env python3
"""Stage per-speech audio for aeneas alignment (AT).

Each AT speech's ``media.videoFileURI`` is the session's HLS master **trimmed
server-side** by ``?startseconds=…&stopseconds=…`` — fetching that URL yields
exactly the per-speech window (verified: ffprobe on a 165 s-window URL returns a
166 s stream). The per-speech MP3/MP4 clip assets are only *sometimes* present
and sometimes cover the whole session, so they are unreliable for alignment;
the trimmed HLS is the canonical per-speech audio.

This is the DE-BY shape (``session_key=None``): transcode each speech's own
(already-trimmed) source straight to its cache file, no shared session download
and no slice. Whole-session / procedural container entries (a "Präsidium" clip
whose window spans hours) are skipped — they exceed any single-speech length and
aeneas would neither finish nor produce a meaningful alignment.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg, prepare_per_speech_audio as _prepare,
)

logger = logging.getLogger(__name__)

# Skip alignment for clips longer than this (whole-session/debate containers).
# Matches the workflow's default --align-max-audio-seconds.
MAX_ALIGN_SECONDS = 2400


def _extract(speech: dict) -> Optional[SpeechAudio]:
    media = speech.get("media") or {}
    hls = media.get("videoFileURI") or ""
    if ".m3u8" not in hls:
        return None
    # Nothing to align without joined text.
    tcs = speech.get("textContents") or []
    if not any((tc.get("textBody") or []) for tc in tcs):
        return None
    duration = media.get("duration")
    if isinstance(duration, (int, float)) and duration > MAX_ALIGN_SECONDS:
        speech.setdefault("debug", {})["alignSkip"] = f"clip-too-long-{int(duration)}s"
        return None
    return SpeechAudio(source_url=hls, session_key=None)


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    # The HLS URL is already trimmed to the speech window; transcode it whole.
    # A single clip's transcode failure (transient HLS/ffmpeg error) must not
    # abort the whole session's alignment — log and leave the target missing so
    # align_audio simply skips this one speech.
    try:
        download_ffmpeg(url, target, hls=True, reconnect=True, timeout=3600)
    except Exception as e:  # noqa: BLE001
        logger.warning("HLS audio prep failed for %s: %s: %s — skipping this speech",
                       target.name, type(e).__name__, e)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=_download)
