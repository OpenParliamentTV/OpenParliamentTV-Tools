"""Stage per-speech audio for aeneas alignment (IT).

A speech's ``media.videoFileURI`` is a time range of the MP4 of its part of the sitting (``…mp4#t=start,end``). The Chamber publishes a sitting's recording in one quality only, 1080p at several gigabytes per part, as a file and as a stream of the same data; there is no audio rendition. To take the audio out of it, the whole part has to be transferred once.

That is done with one plain request per part: the MP4 is downloaded as a file into the cache, its audio track is written as the session audio the slices are cut from, and the MP4 is deleted. Reading the MP4 through ffmpeg over HTTP instead would send a range request for every stretch of audio between the video data. The server ends a transfer of this size before the file is whole at times; the download is then continued from where it stopped with a further request, a few times at most, and a later run continues what an earlier one left. The download needs the part's size in free disk space while it runs.

The MP4 is ``<session audio>.video.part`` while it is fetched and ``<session audio>.video`` once it is whole, so a run that finds the whole file sends no request. The session audio is written only from a whole MP4 and has to be as long as it; a session audio that is there is therefore the whole recording. The page's last time range can end after the recording does, because the last intervention of a sitting has no end on the page and is given the end the archive states for the sitting.
"""

from __future__ import annotations

import logging
import shutil
import time
from http.client import HTTPException
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
# How far the page's last time range may end after the recording does before that is reported. In the sittings processed the archive's end of the sitting lies 10 to 32 s after the end of the recording.
_END_SLACK_S = 120.0
# How often a transfer that ended before the file was whole is continued within one run, and the wait before each.
_MAX_CONTINUATIONS = 5
_CONTINUE_PAUSE_S = 10.0


class DownloadIncomplete(RuntimeError):
    """The transfer ended before the file was whole. What arrived is kept and can be continued."""


def _video_path(target: Path) -> Path:
    """Where the whole MP4 of a session audio file is kept until its audio is written."""
    return target.with_name(target.name + ".video")


def _partial_path(target: Path) -> Path:
    """Where the MP4 is kept while it is fetched."""
    return target.with_name(target.name + ".video.part")


def _size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def fetch_file(url: str, path: Path, *, opener=urlopen, timeout: float = 120.0) -> int:
    """Download ``url`` to ``path`` with one request, continuing a partial file. Returns the file's size."""
    have = _size(path)
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
        remaining = int(response.headers.get("Content-Length") or 0)
        if have and getattr(response, "status", 200) != 206:
            if remaining == have:
                # The server offers the whole file, and that is what is stored.
                return have
            # The server sends the whole file again: start over.
            have = 0
        free = shutil.disk_usage(path.parent).free
        if remaining + _DISK_RESERVE > free:
            raise RuntimeError(f"not enough disk space for {url}: {remaining / 1e9:.1f} GB to fetch, "
                               f"{free / 1e9:.1f} GB free in {path.parent}")
        logger.info("Downloading %s (%.1f GB%s)", url, remaining / 1e9, ", continued" if have else "")
        with path.open("ab" if have else "wb") as out:
            try:
                while True:
                    chunk = response.read(_CHUNK)
                    if not chunk:
                        break
                    out.write(chunk)
            except (HTTPException, ConnectionError, TimeoutError) as e:
                raise DownloadIncomplete(f"download of {url} was interrupted after {out.tell()} bytes: "
                                         f"{type(e).__name__}: {e}") from e
    size = path.stat().st_size
    if remaining and size != have + remaining:
        raise DownloadIncomplete(f"download of {url} ended early: {size} of {have + remaining} bytes")
    return size


def fetch_whole(url: str, path: Path, *, pause=time.sleep) -> int:
    """Download ``url`` to ``path``, continuing a transfer that ends before the file is whole.

    Each continuation is one more request for the rest of the file. It is made only when the request before brought new data, and at most ``_MAX_CONTINUATIONS`` times; after that the partial file stays for the next run.
    """
    for _ in range(_MAX_CONTINUATIONS):
        before = _size(path)
        try:
            return fetch_file(url, path)
        except DownloadIncomplete as e:
            if _size(path) <= before:
                raise
            logger.warning("%s; continuing from there", e)
            pause(_CONTINUE_PAUSE_S)
    return fetch_file(url, path)


def has_audio(target: Path) -> bool:
    """Whether the session audio is there. It is written from a whole recording only, so what is there is whole."""
    return clip_seconds(target) > 1.0


def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    """The audio track of the MP4 at ``url`` as the session audio ``target``. Nothing is requested when the audio or the whole MP4 is there already."""
    if has_audio(target):
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    video = _video_path(target)
    if not video.exists():
        partial = _partial_path(target)
        fetch_whole(url, partial)
        partial.rename(video)
    length = clip_seconds(video)
    if length <= 1.0:
        raise RuntimeError(f"the recording {url} cannot be read as a media file ({video})")
    target.unlink(missing_ok=True)
    # The audio has to be as long as the recording it is taken from, whatever end the page's last time range names.
    download_ffmpeg(str(video), target, required_duration=length, hls=False, timeout=14400)
    video.unlink(missing_ok=True)
    if length + _END_SLACK_S < required_duration:
        logger.warning("The recording %s is %.0f s long, the page's time ranges reach to %.0f s",
                       url, length, required_duration)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech with text has its audio ready for ``align_audio``."""
    return _prepare(merged_data, cachedir, force=force,
                    extract=fragment_speech_audio, download_session=_download,
                    slice_fn=slice_reencode, two_pass=True)
