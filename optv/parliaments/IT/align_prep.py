"""Stage per-speech audio for aeneas alignment (IT).

A speech's ``media.videoFileURI`` is a time range of the MP4 of its part of the sitting (``…mp4#t=start,end``). The Chamber publishes a sitting's recording in one quality only, 1080p at several gigabytes per part, as a file and as a stream of the same data; there is no audio rendition. To take the audio out of it, the whole part has to be transferred once.

That is done with one plain request per part: the MP4 is downloaded as a file into the cache, its audio track is written as the session audio the slices are cut from, and the MP4 is deleted. Reading the MP4 through ffmpeg over HTTP instead would send a range request for every stretch of audio between the video data. An interrupted download is continued from where it stopped. The download needs the part's size in free disk space while it runs.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from optv.shared.audio_prep import (
    download_ffmpeg, fragment_speech_audio, prepare_per_speech_audio as _prepare, slice_reencode,
)
from optv.shared.http import USER_AGENT
from optv.shared.whisper_diff import clip_seconds

logger = logging.getLogger(__name__)

_CHUNK = 4 * 1024 * 1024
# Free space to keep beside the file while it is downloaded.
_DISK_RESERVE = 2 * 1024 ** 3
# The audio may end this much before the last slice's end and still count as whole.
_AUDIO_TOLERANCE_S = 30.0


def _video_path(target: Path) -> Path:
    """Where the MP4 of a session audio file is kept while it is fetched."""
    return target.with_name(target.name + ".video.part")


def fetch_file(url: str, path: Path, *, opener=urlopen, timeout: float = 120.0) -> int:
    """Download ``url`` to ``path`` with one request, continuing a partial file. Returns the file's size."""
    have = path.stat().st_size if path.exists() else 0
    headers = {"User-Agent": USER_AGENT}
    if have:
        headers["Range"] = f"bytes={have}-"
    try:
        response = opener(Request(url, headers=headers), timeout=timeout)
    except HTTPError as e:
        if have and e.code == 416:
            # Nothing lies behind what is stored: the file was whole already.
            return have
        raise
    with response:
        if have and getattr(response, "status", 200) != 206:
            # The server sends the whole file again: start over.
            have = 0
        remaining = int(response.headers.get("Content-Length") or 0)
        free = shutil.disk_usage(path.parent).free
        if remaining + _DISK_RESERVE > free:
            raise RuntimeError(f"not enough disk space for {url}: {remaining / 1e9:.1f} GB to fetch, "
                               f"{free / 1e9:.1f} GB free in {path.parent}")
        logger.info("Downloading %s (%.1f GB%s)", url, remaining / 1e9, ", continued" if have else "")
        with path.open("ab" if have else "wb") as out:
            while True:
                chunk = response.read(_CHUNK)
                if not chunk:
                    break
                out.write(chunk)
    size = path.stat().st_size
    if remaining and size != have + remaining:
        raise RuntimeError(f"download of {url} ended early: {size} of {have + remaining} bytes")
    return size


def has_audio(target: Path, required_duration: float) -> bool:
    """Whether the session audio is there and reaches as far as the latest slice needs."""
    if not target.exists() or target.stat().st_size == 0:
        return False
    seconds = clip_seconds(target)
    return seconds > 1.0 and seconds + _AUDIO_TOLERANCE_S >= required_duration


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    """The audio track of the MP4 at ``url`` as the session audio ``target``. Nothing is requested when the audio is there already."""
    if has_audio(target, required_duration):
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    video = _video_path(target)
    fetch_file(url, video)
    target.unlink(missing_ok=True)
    download_ffmpeg(str(video), target, required_duration=required_duration, hls=False, timeout=14400)
    video.unlink(missing_ok=True)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech with text has its audio ready for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=fragment_speech_audio, download_session=_download,
                    slice_fn=slice_reencode, two_pass=True)
