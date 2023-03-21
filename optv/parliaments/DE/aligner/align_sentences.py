#! /usr/bin/env python3

"""Time-align sentences from a list of speeches
"""

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
from itertools import groupby
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Iterable, Optional
from urllib.request import urlretrieve

from aeneas.executetask import ExecuteTask
from aeneas.task import Task

# We want to check that we have 1GB minimum available cache size
MIN_CACHE_SPACE = 1024 * 1024 * 1024
DEFAULT_CACHEDIR = '/tmp/cache'

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


def align_audio(source: list, language: str, cachedir: Path = None, force: bool = False) -> list:
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

        # Download media file
        media = mediafile(speech, cachedir, mediatype='audio')
        if media is None:
            # No audio. Try to fallback on video.
            media = mediafile(speech, cachedir, mediatype='video')
        if media is None:
            logger.debug("Can find no audio nor video.")
            continue

        # Generate parsed text format file with identifier + sentence
        sentence_file = cachedfile(speech, 'txt', cachedir)
        with open(sentence_file, 'wt') as sf:
            sf.writelines("|".join((ident, sentence['text'].replace('\n', ' '))) + os.linesep
                          for (ident, sentence) in sentence_list)

        start_time = time.time()
        logger.warning(f"Aligning {sentence_file} with {media}")
        # Do the alignment
        aeneas_options = """task_adjust_boundary_no_zero=false|task_adjust_boundary_nonspeech_min=2|task_adjust_boundary_nonspeech_string=REMOVE|task_adjust_boundary_nonspeech_remove=REMOVE|is_audio_file_detect_head_min=0.1|is_audio_file_detect_head_max=3|is_audio_file_detect_tail_min=0.1|is_audio_file_detect_tail_max=3|task_adjust_boundary_algorithm=aftercurrent|task_adjust_boundary_aftercurrent_value=0.5|is_audio_file_head_length=1"""

        task = Task(config_string=f"""task_language={language}|is_text_type=parsed|os_task_file_format=json|{aeneas_options}""")
        task.audio_file_path_absolute = str(media.absolute())
        task.text_file_path_absolute = str(sentence_file.absolute())
        # process Task
        ExecuteTask(task).execute()
        end_time = time.time()

        # Keep only REGULAR fragments (other can be HEAD/TAIL...)
        fragments = dict(  (f.identifier, f)
                           for f in task.sync_map_leaves()
                           if f.is_regular )

        # Inject timing information back into the source data
        for ident, sentence in speech_sentence_iter(speech):
            sentence['timeStart'] = str(fragments[ident].begin)
            sentence['timeEnd'] = str(fragments[ident].end)

        debug = speech.setdefault('debug', {})
        debug['align-duration'] = end_time - start_time

        # Store 'aligned' state in 'media'

        # Are there any aligned sentences in the speech?
        sentence_list = [ (ident, sentence)
                          for ident, sentence in speech_sentence_iter(speech)
                          if sentence.get('timeStart') is not None ]
        speech['media']['aligned'] = (len(sentence_list) > 0)

        # Cleanup generated files (keep cached media)
        sentence_file.unlink()

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
                    force: bool = False) -> Path:
    with open(sourcefile) as f:
        source = json.load(f)
    output = { "meta": { **source['meta'],
                         'processing': {
                             **source['meta'].get('processing', {}),
                             "align": datetime.now().isoformat('T', 'seconds'),
                         }
                        },
               "data": align_audio(source['data'], language, cachedir, force)
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
    parser.add_argument("--lang", type=str, default="deu",
                        help="Language")
    parser.add_argument("--cache-dir", type=str, default=None,
                        help="Cache directory")
    parser.add_argument("--force", action="store_true",
                        default=False,
                        help="Force alignment, even if all sentences are already aligned.")
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

    align_audiofile(args.source, args.destination, args.lang, args.cache_dir, args.force)
