"""Stage per-speech audio for aeneas alignment (NL).

A speech's ``media.videoFileURI`` is the day's recording cut to the speech by the stream server. Its audio is fetched as that same cut, so the sentence times aeneas finds are times in the clip a player shows. The master playlist lists five video renditions and one audio rendition; only the audio rendition's playlist is given to ffmpeg, which then fetches nothing but audio segments.

Each speech is its own source (``session_key=None``): no shared download, no slicing. The fetch is paced to ``READRATE`` times playback speed: ffmpeg would otherwise pull a sitting's several thousand segments within minutes.

The server answers a range with whole segments, from the one the speech begins in to the one after the one it ends in, so a clip is one to three segments longer than the speech. The staged clip's measured length is therefore written to ``media.duration``: sentence times are positions in the clip, and both the aligner's bounds check and the platform read them against that length.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from optv.shared.audio_prep import (
    SpeechAudio, download_ffmpeg, prepare_per_speech_audio as _prepare,
)
from optv.shared.http import PoliteHTTP
from optv.shared.whisper_diff import clip_seconds

logger = logging.getLogger(__name__)

# The aligner's default limit (--align-max-audio-seconds). The parser cuts turns well below it.
MAX_ALIGN_SECONDS = 2400

# Fetch speed as a multiple of playback speed: with segments of 3.84 s about
# 2.6 segment requests per second.
READRATE = 10

_AUDIO_RENDITION_RE = re.compile(r'^#EXT-X-MEDIA:.*TYPE=AUDIO.*URI="([^"]+)"', re.M)

_http = PoliteHTTP(polite_delay=0.5)


def audio_playlist(master_url: str, master: str) -> str:
    """The address of the audio rendition named in a master playlist, or the master's own when it names none."""
    m = _AUDIO_RENDITION_RE.search(master)
    return urljoin(master_url, m.group(1)) if m else master_url


def _extract(speech: dict) -> Optional[SpeechAudio]:
    media = speech.get("media") or {}
    url = media.get("videoFileURI") or ""
    if ".m3u8" not in url:
        return None
    # Nothing to align without text.
    if not any((tc.get("textBody") or []) for tc in speech.get("textContents") or []):
        return None
    duration = media.get("duration")
    if isinstance(duration, (int, float)) and duration > MAX_ALIGN_SECONDS:
        speech.setdefault("debug", {})["alignSkip"] = f"clip-too-long-{int(duration)}s"
        return None
    return SpeechAudio(source_url=url, session_key=None)


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    # One speech's audio failing must not stop the session: log it and leave the
    # target missing, so the aligner skips this speech.
    try:
        source = audio_playlist(url, _http.fetch_text(url, retry_count=5))
        download_ffmpeg(source, target, hls=True, reconnect=True, timeout=3600, readrate=READRATE)
    except Exception as e:  # noqa: BLE001
        logger.warning("Audio for %s could not be staged: %s: %s", target.name, type(e).__name__, e)


def _write_clip_length(speech: dict, target: Path) -> None:
    """Set ``media.duration`` to the length of the staged clip; a clip that could not be staged leaves it as merged."""
    seconds = clip_seconds(target)
    if seconds > 0:
        speech.setdefault("media", {})["duration"] = round(seconds, 2)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech with text has its audio ready for ``align_audio``, and its ``media.duration`` set to the clip's length."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=_extract, download_session=_download,
                    on_prepared=_write_clip_length, on_existing=_write_clip_length)
