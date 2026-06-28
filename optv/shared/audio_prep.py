#! /usr/bin/env python3
"""Shared per-speech audio preparation for aeneas alignment.

``optv.shared.align.align_audio`` aligns one audio file per speech: it first
looks for a cached ``cache/audio/{period}{session:03d}{speechIndex}.mp3``
(``align.cachedfile``) and only downloads ``media.audioFileURI`` on a miss.
Parliaments whose source is a long session/debate stream therefore pre-slice:
download the session audio once, cut a per-speech mp3 into that exact path, and
the shared aligner runs unmodified on the cache hit.

This module is the single driver every such parliament uses. Each parliament
supplies a small ``extract`` adapter (how to read ``(source_url, session_key,
start, duration)`` off a speech) plus a download/slice choice; the driver owns
the common skeleton (group-by-session, download-once, slice, cache accounting,
skip/stale handling) and standardises the previously-divergent naming:

* session audio lives under ``cache/audio_session/``, keyed by each parliament's
  own stable id (``md5(url)`` fallback);
* in-progress downloads use a ``…​.part.mp3`` temp name.

Per-parliament *behaviour* differences are preserved as adapter parameters:
``-c copy`` vs re-encode slicing, urllib vs ffmpeg download, the resume/guard
(promoted here from the old FR-only implementation), and optional writeback /
debug-annotation hooks.
"""

from __future__ import annotations

import hashlib
import logging
import re
import subprocess
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from optv.shared.align import align_audio, cachedfile

logger = logging.getLogger(__name__)

# A session download counts as complete if it reaches within this margin of the
# latest offset any of its clips needs (HLS muxing can land a touch short).
_SESSION_AUDIO_TOLERANCE_S = 30.0


@dataclass
class SpeechAudio:
    """What the driver needs to stage one speech's audio.

    ``session_key is None`` marks a per-speech source (e.g. DE-BY's individual
    HLS clip): the driver transcodes the whole source straight to the speech's
    cache file, with no shared session download and no slice.
    """
    source_url: str
    start: float = 0.0
    duration: Optional[float] = None
    session_key: Optional[str] = None


def md5_key(value: str) -> str:
    """Stable cache key from an arbitrary string (URL fallback for session ids)."""
    return hashlib.md5((value or "").encode()).hexdigest()


# --------------------------------------------------------------------------- #
# Path helpers
# --------------------------------------------------------------------------- #

def _part_path(target: Path) -> Path:
    """``foo.mp3`` → ``foo.part.mp3`` (ffmpeg infers the format from the ext)."""
    return target.with_name(f"{target.stem}.part{target.suffix}")


def _sidecar_path(target: Path, tag: str) -> Path:
    """``foo.mp3`` → ``foo.{tag}.mp3`` (resume sidecars: tail, combined, …)."""
    return target.with_name(f"{target.stem}.{tag}{target.suffix}")


def _migrate_legacy_part_names(target: Path) -> None:
    """Older runs used ``foo.mp3.part``; rename once to ``foo.part.mp3``."""
    legacy = target.with_suffix(target.suffix + ".part")
    part = _part_path(target)
    if legacy.exists() and not part.exists():
        legacy.rename(part)


def _duration_seconds(path: Path) -> float:
    if not path.exists() or path.stat().st_size == 0:
        return 0.0
    cp = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nk=1:nw=1", str(path)],
        capture_output=True, text=True, timeout=120,
    )
    if cp.returncode != 0:
        return 0.0
    try:
        return float((cp.stdout or "0").strip() or 0.0)
    except ValueError:
        return 0.0


def _is_complete(local_dur: float, required_duration: float) -> bool:
    if required_duration <= 0:
        return local_dur > 1.0
    return local_dur + _SESSION_AUDIO_TOLERANCE_S >= required_duration


# --------------------------------------------------------------------------- #
# Download building blocks (chosen per parliament)
# --------------------------------------------------------------------------- #

def download_http(url: str, target: Path, *, required_duration: float = 0.0) -> None:
    """Plain HTTP download (e.g. SE, whose source is already an mp3 file)."""
    if target.exists() and target.stat().st_size > 0:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _part_path(target)
    logger.info("Downloading %s → %s", url, target.name)
    try:
        urllib.request.urlretrieve(url, str(tmp))
        tmp.rename(target)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _ffmpeg_extract(url: str, dest: Path, *, hls: bool, sample_rate: int, bitrate: str,
                    reconnect: bool, timeout: int, start_s: float = 0.0) -> None:
    cmd = ["ffmpeg", "-y", "-loglevel", "error"]
    if reconnect:
        cmd += ["-reconnect", "1", "-reconnect_streamed", "1",
                "-reconnect_delay_max", "30", "-rw_timeout", "60000000"]
    if hls:
        cmd += ["-protocol_whitelist", "file,http,https,tcp,tls,crypto"]
    if start_s > 0:
        cmd += ["-ss", f"{start_s}"]
    cmd += ["-i", url, "-vn", "-ac", "1", "-ar", str(sample_rate),
            "-c:a", "libmp3lame", "-b:a", bitrate, "-f", "mp3", str(dest)]
    res = subprocess.run(cmd, capture_output=True, timeout=timeout)
    if res.returncode != 0:
        raise RuntimeError(
            f"ffmpeg extract failed (rc={res.returncode}) for {url}: "
            f"{res.stderr.decode(errors='replace')[:500]}")


def _concat_mp3(parts: list[Path], out: Path) -> None:
    list_file = out.with_suffix(".concat.txt")
    try:
        list_file.write_text(
            "".join(f"file '{p.resolve()}'\n" for p in parts), encoding="utf-8")
        res = subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
             "-i", str(list_file), "-c", "copy", str(out)],
            capture_output=True, timeout=600)
        if res.returncode != 0:
            raise RuntimeError(
                f"ffmpeg concat failed (rc={res.returncode}): "
                f"{res.stderr.decode(errors='replace')[:300]}")
    finally:
        list_file.unlink(missing_ok=True)


def download_ffmpeg(url: str, target: Path, *, required_duration: float = 0.0,
                    hls: bool = False, sample_rate: int = 22050, bitrate: str = "64k",
                    reconnect: bool = False, timeout: int = 3600) -> None:
    """ffmpeg download → mono mp3, with resume + truncation guard.

    Promoted from the former FR-only implementation so every parliament shares
    one resume-capable downloader. With ``required_duration=0`` (the default for
    parliaments that never needed a guard) any >1 s result is accepted, and the
    resume path still kicks in when a ``.part.mp3`` from a prior run is present.
    Works for both HLS masters (``hls=True``) and plain mp4 (``video_to_audio``
    via the always-present ``-vn``).
    """
    if target.exists() and target.stat().st_size > 0 and _is_complete(
            _duration_seconds(target), required_duration):
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    _migrate_legacy_part_names(target)
    part = _part_path(target)

    if part.exists() and part.stat().st_size > 0:
        part_dur = _duration_seconds(part)
        if _is_complete(part_dur, required_duration):
            part.rename(target)
            return
        # Resume: keep the partial, fetch the remainder from its end, concat.
        logger.info("Resuming download %s from %.1fs (need %.1fs)",
                    target.name, part_dur, required_duration)
        segments = [part]
        tail = _sidecar_path(target, "tail")
        if tail.exists() and tail.stat().st_size > 0:
            segments.append(tail)
        total = sum(_duration_seconds(s) for s in segments)
        if not _is_complete(total, required_duration):
            dest = _sidecar_path(target, "tail_cont")
            if not (dest.exists() and dest.stat().st_size > 0):
                _ffmpeg_extract(url, dest, hls=hls, sample_rate=sample_rate,
                                bitrate=bitrate, reconnect=reconnect, timeout=timeout,
                                start_s=total)
            if dest.exists() and dest.stat().st_size > 0:
                segments.append(dest)
            total = sum(_duration_seconds(s) for s in segments)
        if not _is_complete(total, required_duration):
            raise RuntimeError(
                f"session audio still too short after resume: {total:.1f}s "
                f"required={required_duration:.1f}s")
        combined = _sidecar_path(target, "combined")
        _concat_mp3(segments, combined)
        combined.replace(target)
        for s in segments:
            s.unlink(missing_ok=True)
        return

    # Fresh download.
    logger.info("Downloading %s → %s", "HLS" if hls else "audio", target.name)
    _ffmpeg_extract(url, part, hls=hls, sample_rate=sample_rate, bitrate=bitrate,
                    reconnect=reconnect, timeout=timeout)
    if not _is_complete(_duration_seconds(part), required_duration):
        raise RuntimeError(
            f"session audio truncated after download: "
            f"{_duration_seconds(part):.1f}s required={required_duration:.1f}s ({url})")
    part.rename(target)


# --------------------------------------------------------------------------- #
# Slice building blocks (chosen per parliament)
# --------------------------------------------------------------------------- #

def slice_copy(session_audio: Path, start: float, duration: float, out: Path) -> None:
    """Stream-copy slice (SE/NO: source already mp3, keyframe drift tolerable)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = _part_path(out)
    cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{start}", "-t", f"{duration}",
           "-i", str(session_audio), "-c", "copy", "-f", "mp3", str(tmp)]
    res = subprocess.run(cmd, capture_output=True)
    if res.returncode != 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"ffmpeg slice (copy) failed (rc={res.returncode}) for {out.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}")
    tmp.rename(out)


def slice_reencode(session_audio: Path, start: float, duration: float, out: Path,
                   *, sample_rate: int = 22050, bitrate: str = "64k") -> None:
    """Re-encoded slice — exact, decoder-aligned timestamps (HLS-sourced mp3s)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = _part_path(out)
    cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{start}", "-t", f"{duration}",
           "-i", str(session_audio), "-vn", "-ac", "1", "-ar", str(sample_rate),
           "-c:a", "libmp3lame", "-b:a", bitrate, "-f", "mp3", str(tmp)]
    res = subprocess.run(cmd, capture_output=True, timeout=300)
    if res.returncode != 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"ffmpeg slice (reencode) failed (rc={res.returncode}) for {out.name}: "
            f"{res.stderr.decode(errors='replace')[:300]}")
    tmp.rename(out)


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #

def prepare_per_speech_audio(
    merged_data: list[dict], cachedir: Path, *, force: bool = False,
    extract: Callable[[dict], Optional[SpeechAudio]],
    download_session: Callable[..., None],
    slice_fn: Callable[[Path, float, float, Path], None] = slice_reencode,
    session_subdir: str = "audio_session",
    two_pass: bool = False,
    on_prepared: Optional[Callable[[dict, Path], None]] = None,
    on_existing: Optional[Callable[[dict, Path], None]] = None,
) -> tuple[int, int, int]:
    """Ensure each speech has a per-speech MP3 ready for ``align_audio``.

    ``extract`` maps a speech to a :class:`SpeechAudio` (or ``None`` to skip,
    optionally annotating ``speech['debug']``). Sessions are downloaded once
    each via ``download_session(url, target, *, required_duration=…)`` and cut
    with ``slice_fn``; a ``session_key`` of ``None`` means a per-speech source
    that is downloaded straight to the speech's cache file with no slice.

    Returns ``(prepared, skipped_existing, skipped_no_data)``.
    """
    cachedir = Path(cachedir)
    session_dir = cachedir / session_subdir

    # Pass 1 (guarded downloads only): the latest offset each session must reach.
    required: dict[str, float] = {}
    if two_pass:
        for speech in merged_data:
            spec = extract(speech)
            if spec is None or spec.session_key is None or spec.duration is None:
                continue
            end = float(spec.start) + float(spec.duration)
            if end > required.get(spec.session_key, 0.0):
                required[spec.session_key] = end

    prepared = skipped_existing = skipped_no_data = 0
    seen: set[str] = set()

    for speech in merged_data:
        spec = extract(speech)
        target = cachedfile(speech, "mp3", cachedir)
        if spec is None:
            # Drop a now-invalid slice from a previous run.
            if target.exists():
                target.unlink()
            skipped_no_data += 1
            continue

        if target.exists() and not force:
            if on_existing is not None:
                on_existing(speech, target)
            skipped_existing += 1
            continue

        if spec.session_key is None:
            # Per-speech source: transcode the whole clip straight to target.
            download_session(spec.source_url, target)
        else:
            # Hash the session key for the on-disk name: session_key is often the
            # raw stream URL (fragment parliaments), which as a filename would turn
            # the slashes/`https:` into a nested path tree. md5 gives a flat,
            # collision-free name (cf. the docstring's "stable id (md5(url))").
            session_audio = session_dir / f"{md5_key(spec.session_key)}.mp3"
            if spec.session_key not in seen:
                download_session(spec.source_url, session_audio,
                                 required_duration=required.get(spec.session_key, 0.0))
                seen.add(spec.session_key)
            slice_fn(session_audio, float(spec.start), float(spec.duration or 0.0), target)

        if on_prepared is not None:
            on_prepared(speech, target)
        prepared += 1
        logger.debug("  prepared %s", target.name)

    logger.info(
        "Audio prep: %d prepared, %d cached, %d skipped (no data); %d session download(s)",
        prepared, skipped_existing, skipped_no_data, len(seen))
    return prepared, skipped_existing, skipped_no_data


# --------------------------------------------------------------------------- #
# Media-fragment adapter (the `<stream>#t=start,end` DE state parliaments)
# --------------------------------------------------------------------------- #

# ``<base-stream>#t=start[,end]`` (start may be negative, e.g. DE-HH "#t=-5,391").
_FRAGMENT_RE = re.compile(r"(?P<base>.*?)#t=(?P<start>-?[\d.]+)(?:,(?P<end>-?[\d.]+))?\s*$")


def fragment_speech_audio(speech: dict, *, require_text: bool = True) -> Optional[SpeechAudio]:
    """Build a :class:`SpeechAudio` from a ``<stream>#t=start,end`` videoFileURI.

    The session-sliced DE state parliaments (DE-BW, DE-HH, DE-NW, DE-SH, DE-SN)
    all address a speech as an HTML5 media fragment on ``media.videoFileURI``:
    the part before ``#t=`` is the shared session/clip stream (the slice source,
    used as ``session_key``) and ``start,end`` are the per-speech offsets into it.

    Only text-bearing speeches are staged (alignment has nothing to do for a
    speech with no joined proceedings text); returns ``None`` to skip otherwise,
    or when no usable end offset is present (e.g. an unbounded trailing speech).
    """
    if require_text:
        tcs = speech.get("textContents") or []
        if not any((tc.get("textBody") or []) for tc in tcs):
            return None
    media = speech.get("media") or {}
    m = _FRAGMENT_RE.match(media.get("videoFileURI") or "")
    if not m or not m.group("base"):
        return None
    start = max(0.0, float(m.group("start")))
    end = m.group("end")
    if end is None:
        # Fall back to an explicit endOffset; without an end we can't bound a slice.
        end = (media.get("additionalInformation") or {}).get("endOffset")
        if end is None:
            return None
    duration = float(end) - start
    if duration <= 0:
        return None
    return SpeechAudio(source_url=m.group("base"), session_key=m.group("base"),
                       start=start, duration=duration)


def make_fragment_prepare(*, hls: bool, reconnect: bool = False, timeout: int = 14400):
    """Build a ``prepare_per_speech_audio(merged_data, cachedir, *, force)``.

    For the ``#t=start,end``-fragment, session-sliced DE state parliaments: the
    shared stream (HLS master for DE-HH/NW/SN, base mp4 for DE-BW/SH) is
    downloaded once per session and re-encode-sliced per speech. ``timeout`` is
    generous because some sources are one long per-session stream (DE-NW serves
    a ~10 h HLS master).
    """
    def _download(url: str, target: Path, *, required_duration: float = 0.0) -> None:
        download_ffmpeg(url, target, required_duration=required_duration,
                        hls=hls, reconnect=reconnect, timeout=timeout)

    def prepare(merged_data: list[dict], cachedir: Path, *,
                force: bool = False) -> tuple[int, int, int]:
        return prepare_per_speech_audio(
            merged_data, cachedir, force=force,
            extract=fragment_speech_audio, download_session=_download,
            slice_fn=slice_reencode, two_pass=True)

    return prepare


# --------------------------------------------------------------------------- #
# Align workflow hook
# --------------------------------------------------------------------------- #

def make_align_hook(prepare_audio: Callable[..., tuple[int, int, int]]):
    """Build an ``align_session_to_file`` workflow hook for a parliament.

    Wraps a parliament's per-speech audio-prep function (e.g. the result of
    :func:`make_fragment_prepare`, or a hand-written ``align_prep`` for a
    per-speech-clip source like DE-BY): it loads the richest available doc
    (published ``processed/`` if present, else the freshest cache), stages
    audio, runs aeneas, stamps ``meta.processing.align`` and writes
    ``<session>-aligned.json``. Every aligning parliament shares this body; the
    only per-parliament delta is how the audio is staged.

    Sourcing the richest doc (not always ``merged``) keeps NER/wids already in
    ``processed/`` in the aligned output, so a re-align only *adds* timing and
    no longer trips the publish demotion guard (which would otherwise silently
    discard a forced re-alignment of an already-NER'd session).
    """
    import datetime as _dt
    import json as _json

    from optv.shared.publish import richest_source

    def _align(config, session, args):
        source_file = richest_source(config, session)
        if not source_file.exists():
            raise FileNotFoundError(f"[{session}] no merged file — cannot align")
        doc = _json.loads(source_file.read_text())

        logger.info("[%s] staging per-speech audio", session)
        prepare_audio(doc["data"], args.cache_dir, force=args.force)

        logger.info("[%s] running aeneas alignment (%s)", session, args.aeneas_language)
        align_audio(doc["data"], language=args.aeneas_language, cachedir=args.cache_dir,
                    force=args.force, timeout=args.align_timeout,
                    max_audio_seconds=args.align_max_audio_seconds)
        now = _dt.datetime.utcnow().isoformat(timespec="seconds")
        doc["meta"].setdefault("processing", {})["align"] = now
        doc["meta"]["lastProcessing"] = "align"
        doc["meta"]["lastUpdate"] = now

        aligned_file = config.file(session, "aligned", create=True)
        aligned_file.write_text(_json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info("[%s] wrote %s", session, aligned_file.name)
        return aligned_file

    return _align
