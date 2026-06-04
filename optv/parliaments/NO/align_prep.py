#! /usr/bin/env python3
"""Pre-slice per-meeting Storting audio into per-speech MP3s for aeneas.

Stortinget publishes one MP4 per "del" (part) of a meeting, each 1–6 hours
long. ``optv.shared.align`` assumes one audio file per speech, so we slice
``[startOffset, startOffset + duration]`` out of the part MP4 and write to
the cache path ``align_audio.cachedfile()`` already looks for
(``cache/audio/{period}{meeting:03d}{speechIndex}.mp3``).

The part MP4 is downloaded once (extracted to MP3 via ffmpeg) and reused
for every speech that lives inside the same part. ``startOffset`` is taken
from ``media.additionalInformation.startOffset``; the part's source MP4
URL is at ``media.additionalInformation.mp4_url``.
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def _per_speech_cache_path(speech: dict, cachedir: Path) -> Path:
    period = speech["electoralPeriod"]["number"]
    meeting = speech["session"]["number"]
    speech_index = speech["speechIndex"]
    return cachedir / "audio" / f"{period}{str(meeting).rjust(3, '0')}{speech_index}.mp3"


def _meeting_audio_path(qbvid: str, mp4_url: str, meeting_audio_dir: Path) -> Path:
    """Identify a per-part audio cache file. Prefer qbvid (stable, short);
    fall back to URL hash if missing."""
    key = qbvid or hashlib.md5(mp4_url.encode()).hexdigest()
    return meeting_audio_dir / f"{key}.mp3"


def _download_audio_from_mp4(mp4_url: str, target: Path) -> None:
    if target.exists() and target.stat().st_size > 0:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    logger.info(f"ffmpeg extracting audio from {mp4_url} → {target.name}")
    # ffmpeg can read directly from HTTP. -vn drops video. We re-encode to
    # MP3 since aeneas expects a single audio file with reliable seeking.
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", mp4_url,
        "-vn", "-ac", "1", "-ab", "64k", "-f", "mp3",
        str(tmp),
    ]
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        if tmp.exists():
            tmp.unlink()
        raise RuntimeError(
            f"ffmpeg extraction failed (rc={res.returncode}) for {target.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}"
        )
    tmp.rename(target)


def _slice(meeting_audio: Path, start: float, duration: float, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    # Use a ``.mp3`` temp name (not ``.mp3.part``) so ffmpeg's muxer can infer
    # the output format. Also pass ``-f mp3`` explicitly for belt-and-braces.
    tmp = out.with_name(out.stem + ".part.mp3")
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{start}",
        "-t", f"{duration}",
        "-i", str(meeting_audio),
        "-c", "copy", "-f", "mp3",
        str(tmp),
    ]
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        if tmp.exists():
            tmp.unlink()
        raise RuntimeError(
            f"ffmpeg slice failed (rc={res.returncode}) for {out.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}"
        )
    tmp.rename(out)


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for align_audio.

    Returns (prepared, skipped_existing, skipped_no_data).
    """
    cachedir = Path(cachedir)
    meeting_audio_dir = cachedir / "audio_meeting"

    prepared = skipped_existing = skipped_no_data = 0
    extracted: dict[Path, bool] = {}

    for speech in merged_data:
        media = speech.get("media") or {}
        addinfo = media.get("additionalInformation") or {}
        # Prefer the low-bitrate audio source URL written by the merger —
        # falls back to the high-bitrate platform URL for backwards-compat
        # with merged files that pre-date the audio_source_url field.
        mp4_url = addinfo.get("audio_source_url") or addinfo.get("mp4_url")
        qbvid = addinfo.get("qbvid") or ""
        start_offset = addinfo.get("startOffset")
        duration = media.get("duration")
        if not mp4_url or start_offset is None or not duration:
            skipped_no_data += 1
            continue

        target = _per_speech_cache_path(speech, cachedir)
        if target.exists() and not force:
            # Already on disk — also set audioFileURI so the platform's
            # downstream tooling can locate the per-speech MP3 (the merger
            # left it absent because the slice didn't exist yet).
            media["audioFileURI"] = str(target)
            skipped_existing += 1
            continue

        meeting_audio = _meeting_audio_path(qbvid, mp4_url, meeting_audio_dir)
        if meeting_audio not in extracted:
            _download_audio_from_mp4(mp4_url, meeting_audio)
            extracted[meeting_audio] = True

        _slice(meeting_audio, float(start_offset), float(duration), target)
        media["audioFileURI"] = str(target)
        prepared += 1
        logger.debug(f"  sliced {target.name} ({duration}s @ +{start_offset}s)")

    logger.info(
        f"Audio prep: {prepared} sliced, {skipped_existing} cached, "
        f"{skipped_no_data} skipped (no data); {len(extracted)} per-part extract(s)"
    )
    return prepared, skipped_existing, skipped_no_data
