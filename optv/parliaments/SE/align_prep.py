#! /usr/bin/env python3
"""
Pre-slice per-debate Riksdag audio into per-speech MP3s for aeneas.

Riksdag publishes one MP3 per debate, ~40 min covering 5–40 speeches. The
shared ``optv.shared.align`` module assumes one audio file per speech (DE
ships per-speech MP4). Calling it directly against SE data would feed
aeneas the whole-debate audio against a single speaker's text — guaranteed
mis-alignment.

Strategy: for each speech, slice ``[startOffset, startOffset + duration]``
out of the per-debate audio with ``ffmpeg -c copy`` and write it to the
exact cache filename ``align_audio`` looks for
(``cache/audio/{period}{meeting:03d}{speechIndex}.mp3``). On a cache hit
``align_audio.mediafile()`` returns the local file without touching the
network — alignment then runs unmodified.

The per-debate audio is downloaded once and cached under
``cache/audio_debate/{originMediaID}.mp3``. ``startOffset`` is read from
``media.additionalInformation`` as written by ``parsers/media2json.py``.
"""

import hashlib
import logging
import subprocess
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)


def _per_speech_cache_path(speech: dict, cachedir: Path) -> Path:
    """Mirror the filename convention in ``optv.shared.align.cachedfile``."""
    period = speech["electoralPeriod"]["number"]
    meeting = speech["session"]["number"]
    speech_index = speech["speechIndex"]
    return cachedir / "audio" / f"{period}{str(meeting).rjust(3, '0')}{speech_index}.mp3"


def _debate_cache_path(media: dict, debate_audio_dir: Path) -> Path:
    origin_id = media.get("originMediaID") or hashlib.md5(
        (media.get("audioFileURI") or "").encode()
    ).hexdigest()
    return debate_audio_dir / f"{origin_id}.mp3"


def _download_once(url: str, target: Path) -> None:
    if target.exists() and target.stat().st_size > 0:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {url} → {target.name}")
    tmp = target.with_suffix(target.suffix + ".part")
    try:
        urllib.request.urlretrieve(url, str(tmp))
        tmp.rename(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def _slice(debate_audio: Path, start: float, duration: float, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    # -ss before -i is fast-seek; with -c copy this is keyframe-aligned
    # which can drift a few hundred ms — acceptable for aeneas, which
    # adjusts boundaries on its own.
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{start}",
        "-t", f"{duration}",
        "-i", str(debate_audio),
        "-c", "copy",
        str(out),
    ]
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        raise RuntimeError(
            f"ffmpeg slice failed (rc={res.returncode}) for {out.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}"
        )


def prepare_per_speech_audio(merged_data: list[dict], cachedir: Path,
                             *, force: bool = False) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``.

    Returns ``(prepared, skipped_existing, skipped_no_data)``.

    - ``prepared``: speeches whose per-speech mp3 was created in this run.
    - ``skipped_existing``: cache hits (already prepared in a previous run).
    - ``skipped_no_data``: speeches without ``audioFileURI`` or without
      ``startOffset`` / ``duration`` (e.g. media-only entries with missing
      timing — alignment will skip those naturally).
    """
    cachedir = Path(cachedir)
    debate_audio_dir = cachedir / "audio_debate"

    prepared = skipped_existing = skipped_no_data = 0
    # Downloads are amortised across all speeches sharing the same debate.
    seen_debates: set[Path] = set()

    for speech in merged_data:
        media = speech.get("media") or {}
        audio_url = media.get("audioFileURI")
        addinfo = media.get("additionalInformation") or {}
        start_offset = addinfo.get("startOffset")
        duration = media.get("duration")
        if not audio_url or start_offset is None or not duration:
            # Riksdag publishes some procedural interventions (talman/vice
            # talman calls to order) with ``anf_sekunder=0`` despite having
            # transcript text. Slicing a 0-second range yields an empty mp3
            # that aeneas cannot align — skip cleanly here and let the
            # speech's debug carry an explanation.
            if duration == 0:
                speech.setdefault("debug", {})["align-skip"] = "zero-duration-from-source"
                # Remove any stale slice from a previous run so align_audio
                # doesn't trip over an empty/malformed mp3.
                stale = _per_speech_cache_path(speech, cachedir)
                if stale.exists():
                    stale.unlink()
                    logger.debug(f"  removed stale zero-duration slice {stale.name}")
            skipped_no_data += 1
            continue

        target = _per_speech_cache_path(speech, cachedir)
        if target.exists() and not force:
            skipped_existing += 1
            continue

        debate_audio = _debate_cache_path(media, debate_audio_dir)
        if debate_audio not in seen_debates:
            _download_once(audio_url, debate_audio)
            seen_debates.add(debate_audio)

        _slice(debate_audio, float(start_offset), float(duration), target)
        prepared += 1
        logger.debug(f"  sliced {target.name} ({duration}s @ +{start_offset}s)")

    logger.info(
        f"Audio prep: {prepared} sliced, {skipped_existing} cached, "
        f"{skipped_no_data} skipped (no data); {len(seen_debates)} per-debate downloads"
    )
    return prepared, skipped_existing, skipped_no_data
