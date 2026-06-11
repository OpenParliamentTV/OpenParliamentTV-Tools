#! /usr/bin/env python3
"""Stage per-speech DE-BY audio into per-speech MP3s for aeneas.

Unlike the session-stream parliaments (one long stream sliced per speech), each
DE-BY speech already has its OWN HLS stream — the Plenum Online meta_vod item's
``.csmil``, surfaced as the spine's ``media.videoFileURI``. So there is nothing
to slice: we just transcode each speech's stream to a mono mp3 at the cache path
``optv.shared.align`` looks for. The shared driver handles this via a
``session_key=None`` (per-speech, no-slice) :class:`SpeechAudio`.

Text comes from the joined Plenarprotokoll spine (``join_text_to_spine`` in the
merger); only speeches that actually carry text are staged.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg,
    prepare_per_speech_audio as _prepare,
)


def _extract(speech: dict) -> Optional[SpeechAudio]:
    tcs = speech.get("textContents") or []
    if not any((tc.get("textBody") or []) for tc in tcs):
        return None
    url = (speech.get("media") or {}).get("videoFileURI")
    if not url:
        return None
    # Per-speech stream: no shared session, no slice — transcode whole to target.
    return SpeechAudio(source_url=url, session_key=None)


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    download_ffmpeg(url, target, required_duration=required_duration, hls=True)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each text-bearing speech has a per-speech MP3 for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=_download)
