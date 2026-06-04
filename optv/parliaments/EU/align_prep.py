#! /usr/bin/env python3
"""Pre-slice per-sitting EU HLS audio into per-speech MP3s for aeneas.

The EP CDN publishes one HLS stream per plenary sitting (a 10–12h master
playlist with 32+ audio renditions). The shared ``optv.shared.align`` module
expects one downloadable per-speech audio file and would call urllib on the
``audioFileURI`` — but urllib can't follow HLS manifests, so we pre-stage
the cache files here using ffmpeg.

Strategy mirrors ``optv.parliaments.SE.align_prep``:

1. For each unique sitting (keyed by ``media.audioFileURI``), download the
   OR (original/floor) audio track once via ffmpeg → MP3, cached under
   ``cache/audio_debate/{event_ref}.mp3``.
2. For each speech, slice ``[startOffset, startOffset + duration]`` out of
   the sitting MP3 with ``ffmpeg -c copy`` → ``cache/audio/{slug}.mp3`` at
   the path ``optv.shared.align.cachedfile`` looks for. On cache hit, the
   shared aligner skips the download and runs aeneas directly.

The OR audio matches the spoken language of each speech (CRE preserves
original language verbatim), so per-speech aeneas calls just need to use the
matching language code — see ``optv.parliaments.EU.workflow._align``.
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


def _sitting_cache_path(media: dict, sitting_audio_dir: Path) -> Path:
    """One MP3 per sitting (keyed by event-ref, or hashed URL as fallback)."""
    addinfo = media.get("additionalInformation") or {}
    event_ref = addinfo.get("eventRef")
    if not event_ref:
        event_ref = hashlib.md5(
            (media.get("audioFileURI") or "").encode()
        ).hexdigest()
    return sitting_audio_dir / f"{event_ref}.mp3"


def _download_hls_audio(hls_url: str, target: Path) -> None:
    """Download an HLS audio rendition (m3u8) and re-encode to MP3.

    We pass the HLS URL straight to ffmpeg which handles manifest fetching +
    segment concatenation transparently. ``-vn`` strips any video; we
    transcode to MP3 (mono, 22 kHz) — small enough for aeneas to be happy,
    big enough to keep alignment accurate.
    """
    if target.exists() and target.stat().st_size > 0:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    logger.info(f"Downloading HLS → {target.name} ({hls_url})")
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
        "-i", hls_url,
        "-vn",
        "-ac", "1",            # mono
        "-ar", "22050",        # aeneas-friendly sample rate
        "-c:a", "libmp3lame",
        "-b:a", "64k",
        "-f", "mp3",           # explicit format because tmp ends in .part
        str(tmp),
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, timeout=3600)
        if res.returncode != 0:
            raise RuntimeError(
                f"ffmpeg HLS download failed (rc={res.returncode}) for {hls_url}: "
                f"{res.stderr.decode(errors='replace')[:500]}"
            )
        tmp.rename(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def _slice(sitting_audio: Path, start: float, duration: float, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    # -ss before -i is fast-seek. We re-encode here (not -c copy) because the
    # source is a mono mp3 we just produced, and aeneas wants timestamps that
    # match its own decoded audio exactly — keyframe-aligned -c copy slicing
    # can drift several hundred ms on mp3 (no clean keyframes).
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{start}",
        "-t", f"{duration}",
        "-i", str(sitting_audio),
        "-vn",
        "-ac", "1",
        "-ar", "22050",
        "-c:a", "libmp3lame",
        "-b:a", "64k",
        "-f", "mp3",
        str(out),
    ]
    res = subprocess.run(cmd, capture_output=True, timeout=300)
    if res.returncode != 0:
        raise RuntimeError(
            f"ffmpeg slice failed (rc={res.returncode}) for {out.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}"
        )


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``.

    Returns ``(prepared, skipped_existing, skipped_no_data)``.

    The OR audio rendition URL lives in ``media.audioFileURI`` (set by the
    merger). ``startOffset`` and ``duration`` live in ``media.duration`` and
    ``media.additionalInformation.startOffset``.
    """
    cachedir = Path(cachedir)
    sitting_audio_dir = cachedir / "audio_debate"

    prepared = skipped_existing = skipped_no_data = 0
    seen_sittings: set[Path] = set()

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

        sitting_audio = _sitting_cache_path(media, sitting_audio_dir)
        if sitting_audio not in seen_sittings:
            _download_hls_audio(audio_url, sitting_audio)
            seen_sittings.add(sitting_audio)

        _slice(sitting_audio, float(start_offset), float(duration), target)
        prepared += 1
        logger.debug(f"  sliced {target.name} ({duration}s @ +{start_offset}s)")

    logger.info(
        f"Audio prep: {prepared} sliced, {skipped_existing} cached, "
        f"{skipped_no_data} skipped (no data); {len(seen_sittings)} per-sitting downloads"
    )
    return prepared, skipped_existing, skipped_no_data
