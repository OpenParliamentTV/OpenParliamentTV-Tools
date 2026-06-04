#! /usr/bin/env python3
"""Pre-slice per-session FI HLS audio into per-speech MP3s for aeneas.

Eduskunta publishes one HLS stream per plenary session; ``optv.shared.align``
expects one downloadable per-speech audio file. We mirror the EU pipeline:

1. Download the session's HLS audio once via ffmpeg → MP3, cached under
   ``cache/audio_session/{eventRef}.mp3`` (ffmpeg follows the m3u8 manifest;
   ``-vn`` strips video).
2. Slice ``[startOffset, startOffset + duration]`` out of that MP3 →
   ``cache/audio/{slug}.mp3`` at the path ``optv.shared.align.cachedfile``
   looks for, so the shared aligner runs unmodified on a cache hit.

``startOffset`` (= the broadcast ``time``) and ``duration`` are written by the
merger into ``media.additionalInformation`` / ``media.duration``.
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
    addinfo = media.get("additionalInformation") or {}
    event_ref = addinfo.get("eventRef")
    if not event_ref:
        event_ref = hashlib.md5((media.get("audioFileURI") or "").encode()).hexdigest()
    return session_audio_dir / f"{event_ref}.mp3"


def _download_hls_audio(hls_url: str, target: Path) -> None:
    if target.exists() and target.stat().st_size > 0:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    logger.info(f"Downloading HLS → {target.name} ({hls_url})")
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
        "-i", hls_url,
        "-vn", "-ac", "1", "-ar", "22050",
        "-c:a", "libmp3lame", "-b:a", "64k", "-f", "mp3",
        str(tmp),
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, timeout=3600)
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
        "-c:a", "libmp3lame", "-b:a", "64k", "-f", "mp3",
        str(out),
    ]
    res = subprocess.run(cmd, capture_output=True, timeout=300)
    if res.returncode != 0:
        raise RuntimeError(
            f"ffmpeg slice failed (rc={res.returncode}) for {out.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}")


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``.

    Returns ``(prepared, skipped_existing, skipped_no_data)``.
    """
    cachedir = Path(cachedir)
    session_audio_dir = cachedir / "audio_session"

    prepared = skipped_existing = skipped_no_data = 0
    seen_sessions: set[Path] = set()

    for speech in merged_data:
        media = speech.get("media") or {}
        audio_url = media.get("audioFileURI")
        addinfo = media.get("additionalInformation") or {}
        start_offset = addinfo.get("startOffset")
        duration = media.get("duration")
        if not audio_url or start_offset is None or not duration:
            if duration == 0:
                speech.setdefault("debug", {})["align-skip"] = "zero-duration-from-source"
                stale = _per_speech_cache_path(speech, cachedir)
                if stale.exists():
                    stale.unlink()
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
        logger.debug(f"  sliced {target.name} ({duration}s @ +{start_offset}s)")

    logger.info(
        f"Audio prep: {prepared} sliced, {skipped_existing} cached, "
        f"{skipped_no_data} skipped (no data); {len(seen_sessions)} per-session downloads")
    return prepared, skipped_existing, skipped_no_data
