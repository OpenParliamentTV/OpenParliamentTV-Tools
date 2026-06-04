#! /usr/bin/env python3
"""Pre-slice the per-séance HLS audio into per-speech MP3s for aeneas.

The Assemblée nationale publishes one HLS stream per séance (a multi-hour
master playlist). ``optv.shared.align`` expects one downloadable per-speech
audio file and would hand the ``audioFileURI`` to urllib — which can't follow
HLS manifests — so we pre-stage the per-speech cache files with ffmpeg, exactly
mirroring the EU align_prep:

1. Download the séance audio track once via ffmpeg → mono MP3, cached under
   ``cache/audio_session/{crvId}.mp3``.
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
_SESSION_AUDIO_TOLERANCE_S = 30.0


def _per_speech_cache_path(speech: dict, cachedir: Path) -> Path:
    """Mirror the filename convention in ``optv.shared.align.cachedfile``."""
    period = speech["electoralPeriod"]["number"]
    meeting = speech["session"]["number"]
    speech_index = speech["speechIndex"]
    return cachedir / "audio" / f"{period}{str(meeting).rjust(3, '0')}{speech_index}.mp3"


def _session_cache_path(media: dict, session_audio_dir: Path) -> Path:
    addinfo = media.get("additionalInformation") or {}
    crv = addinfo.get("crvId") or media.get("originMediaID")
    if not crv:
        crv = hashlib.md5((media.get("audioFileURI") or "").encode()).hexdigest()
    return session_audio_dir / f"{crv}.mp3"


def _part_path(target: Path) -> Path:
    """``foo.mp3`` → ``foo.part.mp3`` (ffmpeg needs a real ``.mp3`` extension)."""
    return target.with_name(f"{target.stem}.part{target.suffix}")


def _sidecar_path(target: Path, tag: str) -> Path:
    """``foo.mp3`` → ``foo.{tag}.mp3`` (e.g. tail, combined)."""
    return target.with_name(f"{target.stem}.{tag}{target.suffix}")


def _migrate_legacy_part_names(target: Path) -> None:
    """Older runs used ``foo.mp3.part``; rename once to ``foo.part.mp3``."""
    legacy = target.with_suffix(target.suffix + ".part")
    part = _part_path(target)
    if legacy.exists() and not part.exists():
        legacy.rename(part)


def _is_complete(local_dur: float, required_duration: float) -> bool:
    if required_duration <= 0:
        return local_dur > 1.0
    return local_dur + _SESSION_AUDIO_TOLERANCE_S >= required_duration


def _duration_seconds(path: Path) -> float:
    if not path.exists() or path.stat().st_size == 0:
        return 0.0
    cp = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nk=1:nw=1", str(path)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if cp.returncode != 0:
        return 0.0
    try:
        return float((cp.stdout or "0").strip() or 0.0)
    except ValueError:
        return 0.0


def _ffmpeg_hls_to_mp3(hls_url: str, dest: Path, *, start_s: float = 0.0, timeout: int = 10800) -> None:
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
    ]
    if start_s > 0:
        cmd += ["-ss", f"{start_s}"]
    cmd += [
        "-i", hls_url,
        "-vn",
        "-af", "aformat=sample_fmts=s16:channel_layouts=mono",
        "-ac", "1", "-ar", "22050",
        "-c:a", "libmp3lame", "-b:a", "64k",
        "-f", "mp3", str(dest),
    ]
    res = subprocess.run(cmd, capture_output=True, timeout=timeout)
    if res.returncode != 0:
        raise RuntimeError(
            f"ffmpeg HLS download failed (rc={res.returncode}) for {hls_url}: "
            f"{res.stderr.decode(errors='replace')[:500]}")


def _concat_mp3(parts: list[Path], out: Path) -> None:
    list_file = out.with_suffix(".concat.txt")
    try:
        list_file.write_text(
            "".join(f"file '{p.resolve()}'\n" for p in parts),
            encoding="utf-8",
        )
        res = subprocess.run(
            [
                "ffmpeg", "-y", "-loglevel", "error",
                "-f", "concat", "-safe", "0", "-i", str(list_file),
                "-c", "copy", str(out),
            ],
            capture_output=True,
            timeout=600,
        )
        if res.returncode != 0:
            raise RuntimeError(
                f"ffmpeg concat failed (rc={res.returncode}): "
                f"{res.stderr.decode(errors='replace')[:300]}"
            )
    finally:
        list_file.unlink(missing_ok=True)


def _download_hls_audio(hls_url: str, target: Path, *, required_duration: float) -> None:
    if target.exists() and target.stat().st_size > 0:
        if _is_complete(_duration_seconds(target), required_duration):
            return
    target.parent.mkdir(parents=True, exist_ok=True)
    _migrate_legacy_part_names(target)
    part = _part_path(target)
    if part.exists() and part.stat().st_size > 0:
        part_dur = _duration_seconds(part)
        if _is_complete(part_dur, required_duration):
            logger.warning(
                "Reusing existing partial séance audio %s (%.1fs, need %.1fs)",
                part.name, part_dur, required_duration,
            )
            part.rename(target)
            return
        # Resume from existing partial instead of restarting from 0.
        logger.info(
            "Resuming séance download %s from %.1fs (need %.1fs)",
            target.name, part_dur, required_duration,
        )
        tail = _sidecar_path(target, "tail")
        combined = _sidecar_path(target, "combined")
        segments: list[Path] = [part]
        if tail.exists() and tail.stat().st_size > 0:
            segments.append(tail)
        total = sum(_duration_seconds(s) for s in segments)
        if not _is_complete(total, required_duration):
            offset = total
            dest = _sidecar_path(target, "tail_cont")
            if not dest.exists() or dest.stat().st_size == 0:
                logger.info("Downloading from +%.1fs -> %s (keeping prior segments)", offset, dest.name)
                _ffmpeg_hls_to_mp3(hls_url, dest, start_s=offset)
            if dest.exists() and dest.stat().st_size > 0:
                segments.append(dest)
            total = sum(_duration_seconds(s) for s in segments)
        if not _is_complete(total, required_duration):
            raise RuntimeError(
                f"session audio still too short after resume: {total:.1f}s "
                f"required={required_duration:.1f}s"
            )
        _concat_mp3(segments, combined)
        combined.replace(target)
        logger.info("Séance download complete: %s (%.1fs)", target.name, total)
        return
    logger.info(f"Downloading séance HLS audio → {target.name}")
    try:
        _ffmpeg_hls_to_mp3(hls_url, part)
        local_dur = _duration_seconds(part)
        if not _is_complete(local_dur, required_duration):
            raise RuntimeError(
                f"session audio truncated after download: local={local_dur:.1f}s "
                f"required={required_duration:.1f}s ({hls_url})"
            )
        part.rename(target)
    except Exception:
        # Keep .part for post-mortem/reuse and avoid throwing away nearly-complete downloads.
        raise


def _slice(session_audio: Path, start: float, duration: float, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{start}", "-t", f"{duration}",
        "-i", str(session_audio),
        "-vn",
        "-af", "aformat=sample_fmts=s16:channel_layouts=mono",
        "-ac", "1", "-ar", "22050",
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
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``."""
    cachedir = Path(cachedir)
    session_audio_dir = cachedir / "audio_session"

    prepared = skipped_existing = skipped_no_data = 0
    seen_sessions: set[Path] = set()
    required_session_duration: dict[Path, float] = {}

    for speech in merged_data:
        media = speech.get("media") or {}
        audio_url = media.get("audioFileURI")
        addinfo = media.get("additionalInformation") or {}
        start_offset = addinfo.get("startOffset")
        duration = media.get("duration")
        if not audio_url or start_offset is None or duration is None:
            continue
        if float(duration) < 0.1:
            continue
        session_audio = _session_cache_path(media, session_audio_dir)
        required_end = float(start_offset) + float(duration)
        prev = required_session_duration.get(session_audio, 0.0)
        if required_end > prev:
            required_session_duration[session_audio] = required_end

    for speech in merged_data:
        media = speech.get("media") or {}
        audio_url = media.get("audioFileURI")
        addinfo = media.get("additionalInformation") or {}
        start_offset = addinfo.get("startOffset")
        duration = media.get("duration")
        if not audio_url or start_offset is None or duration is None or float(duration) < 0.1:
            if duration == 0:
                speech.setdefault("debug", {})["align-skip"] = "zero-duration-from-source"
            elif duration is not None and float(duration) < 0.1:
                speech.setdefault("debug", {})["align-skip"] = "sub-100ms-duration"
            skipped_no_data += 1
            continue

        target = _per_speech_cache_path(speech, cachedir)
        if target.exists() and not force:
            skipped_existing += 1
            continue

        session_audio = _session_cache_path(media, session_audio_dir)
        if session_audio not in seen_sessions:
            required_duration = required_session_duration.get(session_audio, 0.0)
            _download_hls_audio(audio_url, session_audio, required_duration=required_duration)
            seen_sessions.add(session_audio)

        try:
            _slice(session_audio, float(start_offset), float(duration), target)
        except Exception as ex:  # noqa: BLE001
            logger.warning(
                f"slice failed for speech {speech.get('speechIndex')} "
                f"({duration}s @ +{start_offset}s): {ex}"
            )
            continue
        prepared += 1

    logger.info(
        f"Audio prep: {prepared} sliced, {skipped_existing} cached, "
        f"{skipped_no_data} skipped (no data); {len(seen_sessions)} séance download(s)")
    return prepared, skipped_existing, skipped_no_data
