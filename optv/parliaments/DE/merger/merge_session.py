#! /usr/bin/env python3

# Merge proceeding and media files

# It takes as input a proceeding file/dir and a media file/dir and outputs a third one with speeches merged.

import logging
logger = logging.getLogger('merge_session' if __name__ == '__main__' else __name__)

import argparse
from copy import deepcopy
import itertools
import json
from pathlib import Path
import sys
from typing import Optional, Tuple, Iterable
import unicodedata

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

def merge_item(mediaitem, proceedingitems):
    # We have both items - copy proceedings data into media item
    # Make a copy of the media data
    output = deepcopy(mediaitem)

    first_proceeding = proceedingitems[0]
    # Copy relevant data from proceedings
    output['agendaItem']['proceedingIndex'] = first_proceeding['agendaItem']['speechIndex']
    output['agendaItem']['proceedingIndexes'] = [ p['agendaItem']['speechIndex'] for p in proceedingitems ]
    output['agendaItem']['mediaIndex'] = mediaitem['agendaItem']['speechIndex']
    # Merge people in case of multiple proceedings
    # FIXME: we do a simple append for now, we should filter out duplicated elements, but preserve
    # order
    output['people'] = [ person
                         for p in proceedingitems
                         for person in p['people'] ]
    # Merge textContents from all proceeedings
    output['textContents'] = [ tc
                               for p in proceedingitems
                               for tc in p['textContents'] ]
    output['documents'] = [ doc
                            for p in proceedingitems
                            for doc in p['documents'] ]
    return output

def speaker_cleanup(item, default_value):
    if item.get('people'):
        # Warning: we use people[0] assuming it is the main
        # speaker. It works because proceedings2json (now) explicitly
        # sorts the people list
        speaker = remove_accents(item['people'][0]['label'].lower()).replace(' von der ', ' ').replace('altersprasident ', '')
    else:
        speaker = None
    return speaker

def needleman_wunsch_align(proceedings, media, options):
    """Align data structures using Needleman-Wunsch algorithm
    """
    config = {
        "speaker_weight": 4,
        "title_weight": 2,
        "merge_penalty": -1,
        "split_penalty": -1,
    };
    def build_index(l):
        return [
            { "index": item['agendaItem']['speechIndex'],
              "speaker": item['people'][0]['label'] if item['people'] else "NO_SPEAKER",
              "title": item['agendaItem']['officialTitle'],
            "item": item }
            for item in l
        ]
    media_index = build_index(media)
    proceedings_index = build_index(proceedings)

    # Levenshtein has been tested, but gives worse results, because
    # the differences are too small (last character for TOP)
    def string_similarity(s1, s2):
        return s1.strip() == s2.strip()

    # Similarity score between 2 items
    def similarity(m, p):
        return (config['speaker_weight'] * string_similarity(m['speaker'], p['speaker'])
                + config['title_weight'] * string_similarity(m['title'], p['title']))

    # Build the [m, p] matrix with scores using the Needleman-Wunsch algorithm
    # https://fr.wikipedia.org/wiki/Algorithme_de_Needleman-Wunsch
    # Initialize a m x p matrix
    scores = [ [ similarity(m, p) for p in proceedings_index ] for m in media_index ]
    # Or 0-initialization?
    # scores = [ [ 0 for p in proceedings_index ] for m in media_index ]

    # Build the score matrix
    for i in range(1, len(media_index)):
        for j in range(1, len(proceedings_index)):
            scores[i][j] = max( scores[i-1][j-1] + similarity(media_index[i], proceedings_index[j]),
                                scores[i-1][j] + config['split_penalty'],
                                scores[i][j-1] + config['merge_penalty'] )

    # Now that the matrix is built, compute a path with a maximal score
    path = []
    i = len(media_index) - 1
    j = len(proceedings_index) - 1
    max_score = scores[i][j]
    while i > 0 and j > 0:
        path.append({ "media_index": i,
                      "proceeding_index": j,
                      "score": max_score,
                      "media": media_index[i]['item'],
                      "proceeding": proceedings_index[j]['item'],
                     })
        diagonal = scores[i - 1][j - 1];
        up = scores[i][j - 1];
        left = scores[i - 1][j];
        if diagonal >= up and diagonal >= left:
            i = i - 1
            j = j - 1
        elif left >= up:
            i = i - 1
        else:
            j = j - 1
    return path

def merge_data(proceedings, media, options):
    """Merge data structures.

    If no match is found for a proceedings, we will dump the
    proceedings as-is.
    """
    path = needleman_wunsch_align(proceedings, media, options)

    # Group by media. There can be multiple proceedings
    return [
        merge_item(group[0]['media'],
                   [ i['proceeding'] for i in group ])
        for group in [ list(group)
                       for media_index, group in itertools.groupby(path, lambda i: i['media_index']) ]
    ]

def matching_proceeding(mediafile: Path, proceedings_dir: Path) -> Optional[Path]:
    p = proceedings_dir / mediafile.name.replace('media', 'proceedings')
    if p.exists():
        return p
    else:
        return None

def build_pairs(proceedings_dir, media_dir) -> Iterable[Tuple[Optional[Path], Optional[Path]]]:
    for m in sorted(media_dir.glob('[0-9]*.json')):
        # Try to find the matching proceedings file
        p = matching_proceeding(m, proceedings_dir)
        yield (p, m)

def merge_files(proceedings_file, media_file, options):
    with open(proceedings_file) as f:
        proceedings = json.load(f)
    with open(media_file) as f:
        media = json.load(f)
    # Order media, according to dateStart
    return merge_data(proceedings, media, options)

def merge_files_or_dirs(media: Path, proceedings: Path, merged_dir: Path, args) -> list[Path]:
    """Merge files or files from directory into merged_dir

    Returns a list of tuples (session:str, filename) for produced merged files.
    """
    pairs = [ (proceedings, media) ]
    if media.is_dir() and proceedings.is_dir():
        # Directory version. Build the pairs data structure
        pairs = list(build_pairs(proceedings, media))
    elif media.is_file() and proceedings.is_dir():
        # Try to find the matching proceedings given a media file.
        pairs = [ (matching_proceeding(media, proceedings), media) ]
    elif media.is_dir() and proceedings.is_file():
        logger.error("Cannot merge data without a media file")
        return []
        sys.exit(1)

    output = []
    for (p, m) in pairs:
        if p is None:
            logger.debug(f"Media {m.name} without proceeding. Copying file")
            data = json.loads(m.read_text())
        elif m is None:
            logger.debug(f"Proceeding {p.name} without media. Copying file")
            data = json.loads(p.read_text())
        else:
            logger.debug(f"Merging {p.name} and {m.name}")
            data = merge_files(p, m, args)

        if merged_dir:
            merged_dir = Path(merged_dir)
            if not merged_dir.is_dir():
                merged_dir.mkdir(parents=True)
            period = data[0]['electoralPeriod']['number']
            meeting = data[0]['session']['number']
            session = f"{period}{str(meeting).rjust(3, '0')}"
            filename = f"{session}-merged.json"
            merged_file = merged_dir / filename

            # Check dates
            # Only save if media or proceedings is newer than merged
            if (not merged_file.exists()
                or merged_file.stat().st_mtime < m.stat().st_mtime
                or ( p is not None and merged_file.stat().st_mtime < p.stat().st_mtime)):
                logger.info(f"Saving into {filename}")
                with open(merged_file, 'w') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                output.append( (session, merged_file) )
            else:
                logger.debug(f"{filename} seems up-to-date")
        else:
            json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
    return output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge proceedings and media file.")
    parser.add_argument("proceedings_file", type=str, nargs='?',
                        help="Proceedings file or directory")
    parser.add_argument("media_file", type=str, nargs='?',
                        help="Media file or directory")
    parser.add_argument("--debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    parser.add_argument("--output", metavar="DIRECTORY", type=str,
                        help="Output directory - if not specified, output with be to stdout")

    args = parser.parse_args()
    if args.media_file is None or args.proceedings_file is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel=logging.DEBUG
    logging.basicConfig(level=loglevel)

    merge_files_or_dirs(Path(args.media_file), Path(args.proceedings_file), args.output, args)
