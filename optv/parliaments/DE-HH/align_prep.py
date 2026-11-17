#! /usr/bin/env python3
"""Stage per-speech DE-HH audio into per-speech MP3s for aeneas.

Each DE-HH speech's ``media.videoFileURI`` is its own server-cut HLS clip, so
there is nothing to slice: each text-bearing speech's clip is transcoded to a
mono mp3 at the cache path ``optv.shared.align`` looks for (the shared driver's
``session_key=None`` per-speech mode, as for DE-BY). Aeneas thus aligns against
exactly the clip the player plays.

Only speeches whose text needs forced alignment are staged: the joined
Plenarprotokoll text (``proceedings``). Subtitle text arrives already timed and
is stamped by the workflow's align hook without audio. Experimental, unvalidated
path (see manifest).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from optv.shared.align import cachedfile
from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg,
    prepare_per_speech_audio as _prepare,
)


def _extract(speech: dict) -> Optional[SpeechAudio]:
    tcs = speech.get("textContents") or []
    if not any(tc.get("type") == "proceedings" and (tc.get("textBody") or []) for tc in tcs):
        return None
    url = (speech.get("media") or {}).get("videoFileURI")
    if not url:
        return None
    return SpeechAudio(source_url=url, session_key=None)


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    download_ffmpeg(url, target, required_duration=required_duration,
                    hls=True, reconnect=True)


def _drop_sliced_audio(merged_data: list[dict], cachedir: Path) -> None:
    """Remove cached audio that was sliced from a longer stream.

    A ``{stem}.window.json`` sidecar marks such a slice; the per-speech driver
    reuses any existing mp3 unchecked, so a slice would otherwise stand in for
    the speech clip (whose start can differ, e.g. a speech starting before its
    agenda item's clip).
    """
    for speech in merged_data:
        target = cachedfile(speech, "mp3", cachedir)
        window = target.with_name(f"{target.stem}.window.json")
        if window.exists():
            target.unlink(missing_ok=True)
            window.unlink()


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech with protocol text has a per-speech MP3 for ``align_audio``."""
    _drop_sliced_audio(merged_data, cachedir)
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=_download)
