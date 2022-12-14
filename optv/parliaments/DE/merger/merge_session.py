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
import unicodedata

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

def merge_item(mediaitem, proceedingitems):
    # We have both items - copy proceedings data into media item
    # Make a copy of the media data
    output = deepcopy(mediaitem)

    first_proceeding = proceedingitems[0]

    # Copy officialDateStart/End from proceedings
    output['session']['officialDateStart'] = first_proceeding['session']['officialDateStart']
    output['session']['officialDateEnd'] = first_proceeding['session']['officialDateEnd']

    # Copy relevant data from proceedings
    output['debug']['proceedingIndex'] = first_proceeding['speechIndex']
    output['debug']['proceedingIndexes'] = [ p['speechIndex'] for p in proceedingitems ]
    output['debug']['mediaIndex'] = mediaitem['speechIndex']

    # Merge people in case of multiple proceedings. We use a dict for
    # de-duplication (instead of a set) so that we preserve order.
    people_dict = dict( (person['label'], person)
                        for p in proceedingitems
                        for person in p['people'] )
    output['people'] = list(people_dict.values())

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
        speaker = default_value
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
            { "index": item['speechIndex'],
              "speaker": speaker_cleanup(item, "NO_SPEAKER"),
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

    # Reverse the path, so that is in ascending order
    path.reverse()

    return path

def merge_data(proceedings, media, options) -> list:
    """Merge data structures.

    If no match is found for a proceedings, we will dump the
    proceedings as-is.
    """
    path = needleman_wunsch_align(proceedings['data'], media['data'], options)

    # Group by media. There can be multiple proceedings
    speeches = [
        merge_item(group[0]['media'],
                   [ i['proceeding'] for i in group ])
        for group in [ list(group)
                       for media_index, group in itertools.groupby(path, lambda i: i['media_index']) ]
    ]
    return { "meta": media['meta'],
             "data": speeches
            }

def merge_files(proceedings_file: Path, media_file:Path, options) -> dict:
    try:
        with open(proceedings_file) as f:
            proceedings = json.load(f)
    except FileNotFoundError:
        proceedings = None
    try:
        with open(media_file) as f:
            media = json.load(f)
    except FileNotFoundError:
        media = None

    if media is None:
        logger.error("No media file for session")
        return dict()
    if proceedings is None:
        logger.debug("No proceedings - return media as temporary merged data")
        return media
    # Order media, according to dateStart
    return merge_data(proceedings, media, options)

def merge_session(session: str, config: "Config", options) -> Path:
    """Merge media/proceeding files for the session.

    Return the produced file Path
    """
    media_file = config.file(session, "media")
    proceedings_file = config.file(session, "proceedings")

    logger.debug(f"Merging {proceedings_file.name} and {media_file.name}")
    output = merge_files(proceedings_file, media_file, options)

    return config.save_data(output, session, "merged")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge proceedings and media files.")
    parser.add_argument("proceedings_file", type=str, nargs='?',
                        help="Proceedings file")
    parser.add_argument("media_file", type=str, nargs='?',
                        help="Media file")
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

    output = merge_files(Path(args.proceedings_file), Path(args.media_file), args)
    if args.output:
        d = Path(args.output) / f"{output['meta']['session']}-merged.json"
        out = open(d, 'w')
    else:
        out = sys.stdout
    json.dump(output, out, indent=2, ensure_ascii=False)

