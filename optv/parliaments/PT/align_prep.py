#! /usr/bin/env python3
"""Pre-slice the per-session HLS audio into per-speech MP3s for aeneas.

av.parlamento.pt serves one continuous HLS stream per reunião (the un-clipped
``…/{session}.mp4/index.m3u8``); each speech carries its ``startOffset`` +
``duration`` (from the av JSON timestamps). ``optv.shared.align`` expects one
downloadable per-speech audio file and would hand the ``audioFileURI`` to urllib
— which can't follow HLS manifests — so we pre-stage the per-speech cache files
with ffmpeg, mirroring the EU/FR align_prep:

1. Download the session audio track once via ffmpeg → mono MP3, cached under
   ``cache/audio_session/{key}.mp3``.
2. Slice ``[startOffset, startOffset + duration]`` out of that file into
   ``cache/audio/{period}{session}{speechIndex}.mp3`` — the exact path
   ``optv.shared.align.cachedfile`` looks for, so the shared aligner finds a
   cache hit and runs aeneas directly.
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def _per_speech_cache_path(speech: dict, cachedir: Path) -> Path:
    """Mirror the filename convention in ``optv.shared.align.cachedfile``."""
    period = speech["electoralPeriod"]["number"]
    meeting = speech["session"]["number"]
    speech_index = speech["speechIndex"]
    return cachedir / "audio" / f"{period}{str(meeting).rjust(3, '0')}{speech_index}.mp3"


def _session_cache_path(media: dict, session_audio_dir: Path) -> Path:
    # All speeches in a reunião share the same session HLS, so the audio URL is a
    # stable per-session key (the av media has no per-session id of its own).
    url = media.get("audioFileURI") or ""
    key = hashlib.md5(url.encode()).hexdigest()
    return session_audio_dir / f"{key}.mp3"


def _download_hls_audio(hls_url: str, target: Path) -> None:
    if target.exists() and target.stat().st_size > 0:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    logger.info(f"Downloading session HLS audio → {target.name}")
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
        "-i", hls_url,
        "-vn", "-ac", "1", "-ar", "22050",
        "-c:a", "libmp3lame", "-b:a", "64k",
        "-f", "mp3", str(tmp),
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, timeout=7200)
        if res.returncode != 0:
            raise RuntimeError(
                f"ffmpeg HLS download failed (rc={res.returncode}) for {hls_url}: "
                f"{res.stderr.decode(errors='replace')[:500]}")
        tmp.rename(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def _slice(session_audio: Path, start: float, duration: float, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{start}", "-t", f"{duration}",
        "-i", str(session_audio),
        "-vn", "-ac", "1", "-ar", "22050",
        "-c:a", "libmp3lame", "-b:a", "64k",
        "-f", "mp3", str(out),
    ]
    res = subprocess.run(cmd, capture_output=True, timeout=300)
    if res.returncode != 0:
        raise RuntimeError(
            f"ffmpeg slice failed (rc={res.returncode}) for {out.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}")


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech with text has a per-speech MP3 ready for ``align_audio``."""
    cachedir = Path(cachedir)
    session_audio_dir = cachedir / "audio_session"

    prepared = skipped_existing = skipped_no_data = 0
    seen_sessions: set[Path] = set()

    for speech in merged_data:
        # Only speeches that actually carry text need aligning.
        tcs = speech.get("textContents") or []
        if not any((tc.get("textBody") or []) for tc in tcs):
            skipped_no_data += 1
            continue

        media = speech.get("media") or {}
        audio_url = media.get("audioFileURI")
        addinfo = media.get("additionalInformation") or {}
        start_offset = addinfo.get("startOffset")
        duration = media.get("duration")
        if not audio_url or start_offset is None or not duration:
            if duration == 0:
                speech.setdefault("debug", {})["align-skip"] = "zero-duration-from-source"
            skipped_no_data += 1
            continue

        target = _per_speech_cache_path(speech, cachedir)
        if target.exists() and not force:
            skipped_existing += 1
            continue

        session_audio = _session_cache_path(media, session_audio_dir)
        if session_audio not in seen_sessions:
            _download_hls_audio(audio_url, session_audio)
            seen_sessions.add(session_audio)

        _slice(session_audio, float(start_offset), float(duration), target)
        prepared += 1

    logger.info(
        f"Audio prep: {prepared} sliced, {skipped_existing} cached, "
        f"{skipped_no_data} skipped (no data); {len(seen_sessions)} session download(s)")
    return prepared, skipped_existing, skipped_no_data
