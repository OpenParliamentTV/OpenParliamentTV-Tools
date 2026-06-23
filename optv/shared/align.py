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

def speech_sentence_iter(speech: dict) -> Iterable:
    """Iterate over all sentences in a speech, adding a unique identifier.
    """
    speechIndex = speech['speechIndex']
    for contentIndex, content in enumerate(speech.get('textContents', [])):
        for bodyIndex, body in enumerate(content['textBody']):
            # Consider only 'speech' sentences
            if body['type'] == 'speech':
                for sentenceIndex, sentence in enumerate(body.get('sentences', [])):
                    ident = f"s{speechIndex}-{contentIndex}-{bodyIndex}-{sentenceIndex}"
                    yield ident, sentence

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


def convert_video_to_audio(video_path: Path, audio_path: Path) -> bool:
    try:
        result = subprocess.run(
            ['ffmpeg', '-i', str(video_path),
             '-vn', '-ac', '1', '-ab', '64k',
             '-f', 'mp3', '-y', str(audio_path)],
            capture_output=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"ffmpeg conversion failed: {result.stderr.decode()[:500]}")
            return False
        return audio_path.exists()
    except FileNotFoundError:
        logger.error("ffmpeg not found on PATH")
        return False
    except subprocess.TimeoutExpired:
        logger.error(f"ffmpeg timed out converting {video_path}")
        return False


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


def align_audio(source: list, language: str, cachedir: Path = None,
                force: bool = False,
                timeout: int = 1200,
                max_audio_seconds: int = 2400) -> list:
    """Align list of speeches to add timing information to sentences.

    The structure is modified in place, and returned.
    """
    if cachedir is None:
        cachedir = Path(DEFAULT_CACHEDIR)
        logger.warning(f"No cache dir specified - using default {cachedir}")
    else:
        cachedir = Path(cachedir)

    for speech in source:
        # Do we have proceedings data to align?
        sentence_list = [ (ident, sentence) for ident, sentence in speech_sentence_iter(speech) ]
        if len(sentence_list) == 0:
            logger.debug(f"No text data to align - skipping {speech['session']['number']}{speech['speechIndex']}")
            continue

        # Do we have any sentence without timing information?
        timing_required = [ sentence
                            for (ident, sentence) in sentence_list
                            if sentence.get('timeStart') is None ]
        if len(timing_required) == 0 and not force:
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

        # Prefer already-cached media; aeneas reads video via ffmpeg, so a
        # cached mp4 is a valid input and avoids a re-conversion.
        mp3_path = cachedfile(speech, 'mp3', cachedir)
        mp4_path = cachedfile(speech, 'mp4', cachedir)
        from_cache = False
        if mp3_path.exists():
            media = mp3_path
            from_cache = True
        elif mp4_path.exists():
            media = mp4_path
            from_cache = True
        else:
            # New speech: download audio, else fall back to video + convert.
            media = mediafile(speech, cachedir, mediatype='audio')
            if media is None:
                video = mediafile(speech, cachedir, mediatype='video')
                if video is not None:
                    if convert_video_to_audio(video, mp3_path):
                        video.unlink()
                        media = mp3_path
                    else:
                        media = video
        if media is None:
            logger.debug("Can find no audio nor video.")
            continue

        speech_label = f"{speech['session']['number']}/{speech['speechIndex']}"

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
                continue

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
            continue

        # Generate parsed text format file with identifier + sentence
        sentence_file = cachedfile(speech, 'txt', cachedir)
        with open(sentence_file, 'wt') as sf:
            sf.writelines("|".join((ident, sentence['text'].replace('\n', ' ').replace('|', '-'))) + os.linesep
                          for (ident, sentence) in sentence_list)

        sync_out = cachedfile(speech, 'sync.json', cachedir)
        if sync_out.exists():
            sync_out.unlink()

        start_time = time.time()
        logger.warning(f"Aligning {sentence_file} with {media}")
        # Do the alignment. task_max_audio_length is a second line of
        # defence: aeneas itself will refuse if our ffprobe pre-flight was
        # somehow bypassed (ffprobe missing, unreadable container).
        aeneas_options = (
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
            f"task_max_audio_length={max_audio_seconds}"
        )

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
            continue

        # Parent reads fragments back from disk — no pickling of aeneas objects.
        try:
            with open(sync_out) as fh:
                fragments = {f['id']: (f['begin'], f['end'])
                             for f in json.load(fh)['fragments']}
        finally:
            if sync_out.exists():
                sync_out.unlink()

        # Inject timing information back into the source data
        for ident, sentence in speech_sentence_iter(speech):
            pair = fragments.get(ident)
            if pair is None:
                continue
            sentence['timeStart'], sentence['timeEnd'] = pair

        debug = speech.setdefault('debug', {})
        debug['alignDuration'] = time.time() - start_time

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
                    max_audio_seconds: int = 2400) -> Path:
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
                                   max_audio_seconds=max_audio_seconds)
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
