#! /usr/bin/env python3

"""Time-align sentences from a list of speeches
"""

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
from itertools import groupby
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Iterable, Optional
import urllib.request
from urllib.request import urlretrieve

# We want to check that we have 1GB minimum available cache size
MIN_CACHE_SPACE = 1024 * 1024 * 1024
DEFAULT_CACHEDIR = '/tmp/cache'

# Install a browser User-Agent opener so urlretrieve works through Cloudflare
# (Congreso.es returns 403 to the default urllib UA).
_opener = urllib.request.build_opener()
_opener.addheaders = [(
    "User-Agent",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)]
urllib.request.install_opener(_opener)

def content_sentence_iter(speech: dict, contentIndex: int, content: dict) -> Iterable:
    """Iterate over the 'speech' sentences of one textContents entry, adding
    a unique identifier."""
    speechIndex = speech['speechIndex']
    for bodyIndex, body in enumerate(content['textBody']):
        # Consider only 'speech' sentences
        if body['type'] == 'speech':
            for sentenceIndex, sentence in enumerate(body.get('sentences', [])):
                ident = f"s{speechIndex}-{contentIndex}-{bodyIndex}-{sentenceIndex}"
                yield ident, sentence

def speech_sentence_iter(speech: dict) -> Iterable:
    """Iterate over all sentences in a speech, adding a unique identifier.
    """
    for contentIndex, content in enumerate(speech.get('textContents', [])):
        yield from content_sentence_iter(speech, contentIndex, content)

def body_iter(speech: dict) -> Iterable:
    """Iterate over all bodies in a speech
    """
    for contentIndex, content in enumerate(speech.get('textContents', [])):
        for bodyIndex, body in enumerate(content['textBody']):
            yield body

def previous_current_next(iterable):
    """Make an iterator that yields an (previous, current, next) tuple per element.

    Returns None if the value does not make sense (i.e. previous before
    first and next after last).
    Adapted from: https://gist.github.com/mortenpi/9604377
    """
    iterable=iter(iterable)
    prv = None
    cur = next(iterable)
    try:
        while True:
            nxt = next(iterable)
            yield (prv, cur, nxt)
            prv = cur
            cur = nxt
    except StopIteration:
        yield (prv, cur, None)

def cachedfile(speech: dict, extension: str, cachedir: Path) -> Path:
    """Return a filename with given extension
    """
    period = speech['electoralPeriod']['number']
    meeting = speech['session']['number']
    speechIndex = speech['speechIndex']
    filename = f"{period}{str(meeting).rjust(3, '0')}{speechIndex}.{extension}"
    audiodir = cachedir / "audio"
    if not audiodir.is_dir():
        audiodir.mkdir(parents=True)
    return audiodir / filename

def mediafile(speech: dict, cachedir: Path, mediatype='audio') -> Optional[Path]:
    """Get an mediafile for the given dict.

    Either it is already cached (return filename) or download it
    first.

    If anything wrong happens, return None
    """
    extension = 'mp3'
    item = 'audioFileURI'
    if mediatype == 'video':
        extension = 'mp4'
        item = 'videoFileURI'
    media = cachedfile(speech, extension, cachedir)
    if not media.exists():
        # Check that we have enough disk space for caching
        total, used, free = shutil.disk_usage(cachedir)
        if free < MIN_CACHE_SPACE:
            logger.error(f"No enough disk space for cache dir: {free / 1024 / 1024 / 1024} GB")
            return None

        # Not yet cached file - download it
        mediaURI = speech.get('media', {}).get(item)
        if not mediaURI:
            logger.error(f"No {item} for {speech['session']['number']}{speech['speechIndex']}")
            return None
        logger.warning(f"Downloading {mediaURI} into {media.name}")
        try:
            (fname, headers) = urlretrieve(mediaURI, str(media))
        except Exception as e:
            logger.error(f"Cannot download {mediaURI}: {e}")
            return None
    return media


def convert_video_to_audio(video_path: Path, audio_path: Path,
                           track: Optional[int] = None) -> bool:
    """Extract the audio of ``video_path`` as MP3. ``track`` picks the N-th
    audio stream (``-map 0:a:N``); None leaves ffmpeg's default choice."""
    stream = [] if track is None else ['-map', f'0:a:{track}']
    try:
        result = subprocess.run(
            ['ffmpeg', '-i', str(video_path), *stream,
             '-vn', '-ac', '1', '-ab', '64k',
             '-f', 'mp3', '-y', str(audio_path)],
            capture_output=True, timeout=300)
        if result.returncode != 0:
            # The banner comes first; the cause is at the end.
            logger.error(f"ffmpeg conversion failed: {result.stderr.decode()[-500:]}")
            return False
        return audio_path.exists()
    except FileNotFoundError:
        logger.error("ffmpeg not found on PATH")
        return False
    except subprocess.TimeoutExpired:
        logger.error(f"ffmpeg timed out converting {video_path}")
        return False


def audio_stream_count(media: Path) -> Optional[int]:
    """Number of audio streams in ``media`` via ffprobe, or None if unknown."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-select_streams', 'a',
             '-show_entries', 'stream=index', '-of', 'csv=p=0', str(media)],
            capture_output=True, timeout=30, text=True)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return len(result.stdout.split())


def track_audio_file(speech: dict, track: Optional[int], cachedir: Path) -> Path:
    """Cached MP3 for one audio track of a speech. Track None (ffmpeg's
    default stream) and track 0 share the plain ``.mp3`` name, so per-speech
    audio staged by the per-parliament ``align_prep`` adapters is reused."""
    if track:
        return cachedfile(speech, f'a{track}.mp3', cachedir)
    return cachedfile(speech, 'mp3', cachedir)


def stage_audio(speech: dict, cachedir: Path, tracks: list) -> dict:
    """Return {track: (media Path or None, from_cache)} for the given tracks.

    Track None is the single-track path: a cached MP3, else a cached MP4
    (aeneas reads video via ffmpeg), else the speech's audioFileURI, else
    its video converted to MP3. Other tracks are extracted from the video
    (None when the video has no such track, e.g. a clip recorded before an
    interpretation channel existed); a video downloaded here is fetched once
    for all tracks and removed afterwards unless it is itself the audio
    source.
    """
    staged: dict = {}
    mp4_path = cachedfile(speech, 'mp4', cachedir)
    video = None
    downloaded = False
    for track in tracks:
        mp3_path = track_audio_file(speech, track, cachedir)
        if mp3_path.exists():
            staged[track] = (mp3_path, True)
            continue
        if track is None:
            if mp4_path.exists():
                staged[track] = (mp4_path, True)
                continue
            media = mediafile(speech, cachedir, mediatype='audio')
            if media is not None:
                staged[track] = (media, False)
                continue
        if video is None:
            if mp4_path.exists():
                video = mp4_path
            else:
                video = mediafile(speech, cachedir, mediatype='video')
                downloaded = video is not None
        if video is None:
            staged[track] = (None, False)
            continue
        if track:
            streams = audio_stream_count(video)
            if streams is not None and track >= streams:
                logger.warning(f"{video.name} has {streams} audio track(s), "
                               f"no track {track}")
                staged[track] = (None, False)
                continue
        if convert_video_to_audio(video, mp3_path, track):
            staged[track] = (mp3_path, False)
        else:
            staged[track] = (video if track is None else None, False)
    if (downloaded and video.exists()
            and all(media != video for media, _cached in staged.values())):
        video.unlink()
    return staged


def aligned_end_out_of_bounds(max_end: float, declared) -> bool:
    """True if an aligned end runs past the declared media duration.

    aeneas can only emit timecodes within the audio it was handed, so an end
    far beyond the *declared* clip duration means the aligned audio was longer
    than the clip the platform serves. The platform positions hit markers as
    ``timecode / media.duration``, so such timecodes render outside the
    timeline. A small 1.1x+1s tolerance absorbs aeneas tail-boundary rounding.
    Returns False when the duration is unknown/zero (nothing to compare).
    """
    if not isinstance(declared, (int, float)) or declared <= 0:
        return False
    return max_end > declared * 1.1 + 1


def _probe_duration_seconds(media: Path) -> Optional[float]:
    """Return media duration in seconds via ffprobe, or None if it fails."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error',
             '-show_entries', 'format=duration',
             '-of', 'csv=p=0', str(media)],
            capture_output=True, timeout=30, text=True)
        if result.returncode != 0:
            return None
        return float(result.stdout.strip())
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return None


def _aeneas_worker(audio_path: str, text_path: str, out_path: str,
                   language: str, aeneas_options: str) -> None:
    """Run aeneas in a child process; write fragments as JSON to out_path.

    Top-level (picklable under spawn); no closure captures — args are plain
    strings. aeneas Task/SyncMap objects never cross the process boundary.
    """
    from aeneas.executetask import ExecuteTask
    from aeneas.task import Task
    task = Task(config_string=(
        f"task_language={language}|is_text_type=parsed|"
        f"os_task_file_format=json|{aeneas_options}"
    ))
    task.audio_file_path_absolute = audio_path
    task.text_file_path_absolute = text_path
    ExecuteTask(task).execute()
    fragments = [
        {"id": f.identifier, "begin": str(f.begin), "end": str(f.end)}
        for f in task.sync_map_leaves()
        if f.is_regular
    ]
    with open(out_path, 'w') as fh:
        json.dump({"fragments": fragments}, fh)


def _track_groups(speech: dict, audio_track) -> list:
    """[(track, [(ident, sentence), ...]), ...] in textContents order; one
    group with track None when no ``audio_track`` mapping is given."""
    if audio_track is None:
        sentences = list(speech_sentence_iter(speech))
        return [(None, sentences)] if sentences else []
    groups: dict = {}
    for contentIndex, content in enumerate(speech.get('textContents', [])):
        sentences = list(content_sentence_iter(speech, contentIndex, content))
        if sentences:
            track = audio_track(speech, contentIndex, content)
            groups.setdefault(track, []).extend(sentences)
    return list(groups.items())


_AENEAS_OPTIONS = (
    "task_adjust_boundary_no_zero=false|"
    "task_adjust_boundary_nonspeech_min=2|"
    "task_adjust_boundary_nonspeech_string=REMOVE|"
    "task_adjust_boundary_nonspeech_remove=REMOVE|"
    "is_audio_file_detect_head_min=0.1|"
    "is_audio_file_detect_head_max=3|"
    "is_audio_file_detect_tail_min=0.1|"
    "is_audio_file_detect_tail_max=3|"
    "task_adjust_boundary_algorithm=aftercurrent|"
    "task_adjust_boundary_aftercurrent_value=0.5|"
    "is_audio_file_head_length=1|"
)


def _align_pass(speech: dict, sentence_list: list, media: Path, from_cache: bool,
                declared, language: str, cachedir: Path, timeout: int,
                max_audio_seconds: int, speech_label: str,
                track: Optional[int]) -> Optional[float]:
    """Align ``sentence_list`` against ``media`` and inject the timings.

    Returns the aeneas run time in seconds, or None when the pass was skipped
    or failed (``debug.alignError`` says why).
    """
    # Self-heal a stale cache hit: a per-speech clip can't plausibly be much
    # longer than its declared window, so a cached file that probes far
    # longer was staged by an earlier run from the wrong (untrimmed /
    # whole-asset) source. Drop it and skip this pass — the next run
    # re-stages correctly (per-parliament align_prep for the trimmed-HLS
    # parliaments, or mediafile() re-download otherwise). Re-fetching here is
    # avoided on purpose: align_audio's own download path would just re-pull
    # the same whole-asset source. The byte ceiling (~400 kbit/s) keeps this
    # to the handful of anomalously large files — correct clips skip the
    # ffprobe entirely.
    if (from_cache and isinstance(declared, (int, float)) and declared > 0
            and media.stat().st_size > declared * 50000):
        probed = _probe_duration_seconds(media)
        if probed is not None and probed > declared * 1.5 + 5:
            logger.warning(
                f"Dropping stale cached audio for speech {speech_label} "
                f"({media.name}, {media.stat().st_size} bytes, {probed:.0f}s "
                f"vs declared {declared:.0f}s) — will re-stage next run")
            media.unlink(missing_ok=True)
            speech.setdefault('debug', {})['alignError'] = (
                f"stale cached audio dropped ({probed:.0f}s vs declared "
                f"{declared:.0f}s); re-stage pending")
            return None

    # Pre-flight: reject media whose duration exceeds the policy cap.
    # Catches the wrong-URL-to-whole-session bug in milliseconds instead
    # of waiting for the wall-clock timeout.
    duration = _probe_duration_seconds(media)
    if duration is not None and duration > max_audio_seconds:
        try:
            size = media.stat().st_size
        except OSError:
            size = -1
        logger.warning(
            f"Skipping alignment for speech {speech_label} "
            f"({media.name}, {size} bytes, {duration:.0f}s audio): "
            f"exceeds --align-max-audio-seconds={max_audio_seconds}"
        )
        debug = speech.setdefault('debug', {})
        debug['alignError'] = f"audio too long ({duration:.0f}s > {max_audio_seconds}s)"
        return None

    # Generate parsed text format file with identifier + sentence
    suffix = f"a{track}." if track else ""
    sentence_file = cachedfile(speech, f'{suffix}txt', cachedir)
    with open(sentence_file, 'wt') as sf:
        sf.writelines("|".join((ident, sentence['text'].replace('\n', ' ').replace('|', '-'))) + os.linesep
                      for (ident, sentence) in sentence_list)

    sync_out = cachedfile(speech, f'{suffix}sync.json', cachedir)
    if sync_out.exists():
        sync_out.unlink()

    start_time = time.time()
    logger.warning(f"Aligning {sentence_file} with {media}")
    # Do the alignment. task_max_audio_length is a second line of
    # defence: aeneas itself will refuse if our ffprobe pre-flight was
    # somehow bypassed (ffprobe missing, unreadable container).
    aeneas_options = _AENEAS_OPTIONS + f"task_max_audio_length={max_audio_seconds}"

    # Run aeneas in a child process so a CPU-bound hang can actually be
    # killed. signal.alarm() won't interrupt the C-level DTW loop;
    # concurrent.futures cancellation doesn't kill a running task.
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(
        target=_aeneas_worker,
        args=(str(media.absolute()), str(sentence_file.absolute()),
              str(sync_out.absolute()), language, aeneas_options),
    )
    timed_out = False
    exit_code = None
    try:
        proc.start()
        proc.join(timeout)
        if proc.is_alive():
            timed_out = True
            try:
                size = media.stat().st_size
            except OSError:
                size = -1
            logger.error(
                f"aeneas timed out after {timeout}s for speech "
                f"{speech_label} ({media.name}, {size} bytes) — skipping"
            )
            proc.terminate()
            proc.join(5)
            if proc.is_alive():
                proc.kill()
                proc.join(5)
        exit_code = proc.exitcode
        if not timed_out and exit_code != 0:
            logger.error(
                f"aeneas child exited {exit_code} for speech "
                f"{speech_label} ({media.name}) — skipping"
            )
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join(5)
        proc.close()
        if sentence_file.exists():
            sentence_file.unlink()

    if timed_out or exit_code != 0 or not sync_out.exists():
        debug = speech.setdefault('debug', {})
        debug['alignError'] = (
            'timeout' if timed_out else f'exit {exit_code}'
        )
        if sync_out.exists():
            sync_out.unlink()
        return None
    elapsed = time.time() - start_time

    # Parent reads fragments back from disk — no pickling of aeneas objects.
    try:
        with open(sync_out) as fh:
            fragments = {f['id']: (f['begin'], f['end'])
                         for f in json.load(fh)['fragments']}
    finally:
        if sync_out.exists():
            sync_out.unlink()

    # Output-side bound check. aeneas can only emit timecodes within the
    # audio it was handed, so an aligned end far beyond the *declared*
    # media duration means the aligned audio was longer than the clip the
    # platform serves (e.g. an untrimmed/whole-asset source, or a clip the
    # CDN later trimmed). The platform divides timecodes by media.duration
    # to position them, so out-of-range ends render outside the timeline.
    # Drop the whole alignment (don't inject) and flag it — honest, and a
    # corrected re-run can fill it in. This catches the fresh-download path
    # and the moderate 2-3x overruns that the input-side size/probe guards
    # above (calibrated for whole-session assets) let through.
    max_end = 0.0
    for _begin, _end in fragments.values():
        try:
            max_end = max(max_end, float(_end))
        except (TypeError, ValueError):
            continue
    if aligned_end_out_of_bounds(max_end, declared):
        logger.warning(
            f"Discarding alignment for speech {speech_label}: aligned end "
            f"{max_end:.1f}s exceeds declared media duration "
            f"{declared:.0f}s — aligned audio longer than published clip")
        speech.setdefault('debug', {})['alignError'] = (
            f"aligned timecodes exceed media duration "
            f"({max_end:.1f}s > {declared:.0f}s)")
        return None

    # Inject timing information back into the source data
    for ident, sentence in sentence_list:
        pair = fragments.get(ident)
        if pair is None:
            continue
        sentence['timeStart'], sentence['timeEnd'] = pair
    return elapsed


# A clip whose declared duration comes from minute-resolution clock times
# (``debug.coarseTiming``) can be up to a minute longer or shorter.
_COARSE_TOLERANCE = 65


def _refine_coarse_duration(speech: dict, declared, staged: dict):
    """Return the duration to check alignments against.

    For a speech flagged ``debug.coarseTiming``, the staged audio's probed
    length replaces ``media.duration`` when it is within a minute of it: the
    platform positions timecodes as ``timecode / media.duration``, so it
    needs the clip's real length. A length further off is left alone for the
    bounds checks to reject (the staged file is then the suspect).
    """
    if not (speech.get('debug') or {}).get('coarseTiming'):
        return declared
    if not isinstance(declared, (int, float)):
        return declared
    media = next((m for m, _from_cache in staged.values() if m is not None), None)
    probed = _probe_duration_seconds(media) if media is not None else None
    if probed is None or abs(probed - declared) > _COARSE_TOLERANCE:
        return declared
    speech['media']['duration'] = round(probed, 3)
    return probed


def align_audio(source: list, language: str, cachedir: Path = None,
                force: bool = False,
                timeout: int = 1200,
                max_audio_seconds: int = 2400,
                audio_track=None) -> list:
    """Align list of speeches to add timing information to sentences.

    ``audio_track`` optionally maps ``(speech, content_index, content)`` to
    the audio stream of the speech's video that the content's sentences are
    aligned against (``-map 0:a:N``), for media carrying several renditions
    such as the floor audio and a simultaneous interpretation. Contents
    mapped to the same track are aligned in one pass. Without it every
    sentence is aligned against one audio file (ffmpeg's default stream).

    The structure is modified in place, and returned.
    """
    if cachedir is None:
        cachedir = Path(DEFAULT_CACHEDIR)
        logger.warning(f"No cache dir specified - using default {cachedir}")
    else:
        cachedir = Path(cachedir)

    for speech in source:
        # Do we have proceedings data to align?
        groups = _track_groups(speech, audio_track)
        if not groups:
            logger.debug(f"No text data to align - skipping {speech['session']['number']}{speech['speechIndex']}")
            continue

        # Which passes have a sentence without timing information?
        if not force:
            groups = [(track, sentence_list) for track, sentence_list in groups
                      if any(sentence.get('timeStart') is None
                             for _ident, sentence in sentence_list)]
            if not groups:
                logger.debug("All sentences already aligned")
                continue

        # Pre-flight on the *declared* duration, before any download. If the
        # feed already says this speech's media is longer than the cap, skip
        # here: the cache-miss fallback below would otherwise pull the speech's
        # full audioFileURI/videoFileURI (for whole-debate/procedural container
        # entries that is a whole-session asset, hundreds of MB) only for the
        # post-download ffprobe cap to discard it. Entries with no declared
        # duration fall through to that authoritative post-download probe.
        declared = (speech.get('media') or {}).get('duration')
        if isinstance(declared, (int, float)) and declared > max_audio_seconds:
            speech_label = f"{speech['session']['number']}/{speech['speechIndex']}"
            logger.info(
                f"Skipping alignment for speech {speech_label}: declared media "
                f"duration {declared:.0f}s exceeds "
                f"--align-max-audio-seconds={max_audio_seconds} (no download)")
            speech.setdefault('debug', {})['alignError'] = (
                f"audio too long ({declared:.0f}s > {max_audio_seconds}s)")
            continue

        # Prefer already-cached media; download (and convert) otherwise.
        staged = stage_audio(speech, cachedir, [track for track, _ in groups])
        declared = _refine_coarse_duration(speech, declared, staged)
        align_seconds = []
        for track, sentence_list in groups:
            media, from_cache = staged[track]
            if media is None:
                logger.debug("Can find no audio nor video.")
                if track:
                    speech.setdefault('debug', {})['alignError'] = f"no audio for track {track}"
                continue
            speech_label = f"{speech['session']['number']}/{speech['speechIndex']}"
            if track is not None:
                speech_label += f" (audio track {track})"
            elapsed = _align_pass(speech, sentence_list, media, from_cache, declared,
                                  language, cachedir, timeout, max_audio_seconds,
                                  speech_label, track)
            if elapsed is not None:
                align_seconds.append(elapsed)
        if not align_seconds:
            continue

        debug = speech.setdefault('debug', {})
        debug['alignDuration'] = sum(align_seconds)

        # Store 'aligned' state in 'media'
        aligned_sentences = [1
                             for _ident, sentence in speech_sentence_iter(speech)
                             if sentence.get('timeStart') is not None]
        speech['media']['aligned'] = (len(aligned_sentences) > 0)

    # We have aligned all "speech"-type bodies. Go through all speeches and
    # use "speech" timecodes to estimate "comment"-type timecodes.
    for speech in source:
        if not speech.get('textContents'):
            # No text to align
            continue
        for prv, cur_list, nxt in previous_current_next(list(arr)
                                                   for (key, arr) in groupby(body_iter(speech),
                                                                             key=lambda body: body['type']
                                                                             )):
            if prv:
                # prv is the list of previous bodies. Take the last one.
                prv = prv[-1]
            if nxt:
                nxt = nxt[0]
            for cur in cur_list:
                if cur['type'] == 'comment':
                    # Copy timestamps from prv/nxt bodies sentences
                    start = ''
                    end = ''
                    if prv:
                        # Using start timecode of last sentence of previous body
                        start = prv['sentences'][-1].get('timeStart', '')
                    elif nxt:
                        # Using first timecode of first sentence of next body
                        start = nxt['sentences'][0].get('timeStart', '')

                    if nxt:
                        end = nxt['sentences'][0].get('timeEnd', '')
                    elif prv:
                        end = prv['sentences'][-1].get('timeEnd', '')

                    if start:
                        cur['sentences'][0]['timeStart'] = start
                    if end:
                        cur['sentences'][0]['timeEnd'] = end

    return source

def align_audiofile(sourcefile: Path,
                    destinationfile: Path,
                    language: str,
                    cachedir: Path = None,
                    force: bool = False,
                    timeout: int = 1200,
                    max_audio_seconds: int = 2400,
                    audio_track=None) -> Path:
    with open(sourcefile) as f:
        source = json.load(f)
    output = { "meta": { **source['meta'],
                         'processing': {
                             **source['meta'].get('processing', {}),
                             "align": datetime.now().isoformat('T', 'seconds'),
                         }
                        },
               "data": align_audio(source['data'], language, cachedir, force,
                                   timeout=timeout,
                                   max_audio_seconds=max_audio_seconds,
                                   audio_track=audio_track)
              }
    if destinationfile is not None:
        with open(destinationfile, 'w') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
    else:
        json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    return output

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Time-align speech sentences.")
    parser.add_argument("source", metavar="source", type=str, nargs='?',
                        help="Source file (merged format)")
    parser.add_argument("destination", metavar="destination", type=str, nargs='?',
                        help="Destination file")
    parser.add_argument("--lang", type=str, required=True,
                        help="ISO 639-3 language code for aeneas/espeak (e.g. 'deu', 'swe'). "
                             "Usually sourced from manifest.locale.aeneas_language.")
    parser.add_argument("--cache-dir", type=str, default=None,
                        help="Cache directory")
    parser.add_argument("--force", action="store_true",
                        default=False,
                        help="Force alignment, even if all sentences are already aligned.")
    parser.add_argument("--align-timeout", type=int, default=1200,
                        help="Wall-clock timeout (s) for aeneas per speech (default: 1200)")
    parser.add_argument("--align-max-audio-seconds", type=int, default=2400,
                        help="Skip alignment if media duration exceeds this (default: 2400)")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")

    args = parser.parse_args()
    if args.source is None:
        parser.print_help()
        sys.exit(1)

    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel)

    align_audiofile(args.source, args.destination, args.lang, args.cache_dir,
                    args.force,
                    timeout=args.align_timeout,
                    max_audio_seconds=args.align_max_audio_seconds)
