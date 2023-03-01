#! /usr/bin/env python3

# Merge proceeding and media files

# It takes as input a proceeding file/dir and a media file/dir and outputs a third one with speeches merged.

import logging
logger = logging.getLogger('merge_session' if __name__ == '__main__' else __name__)

import argparse
from copy import deepcopy
from datetime import datetime
import itertools
import json
from pathlib import Path
import re
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

    output['originTextID'] = first_proceeding['originTextID']

    # Copy officialDateStart/End from proceedings
    output['session']['dateStart'] = first_proceeding['session']['dateStart']
    output['session']['dateEnd'] = first_proceeding['session']['dateEnd']

    # Copy relevant data from proceedings
    output['debug']['proceedingIndex'] = first_proceeding['speechIndex']
    output['debug']['proceedingIndexes'] = [ p['speechIndex'] for p in proceedingitems ]
    output['debug']['mediaIndex'] = mediaitem['speechIndex']

    # Merge people in case of multiple proceedings. We use a dict for
    # de-duplication (instead of a set) so that we preserve order.  We
    # prepend media-based speaker info so that it always appears first
    # (and he is always tagged 'main-speaker')

    # We do a copy of person info because we will possibly update its
    # context info (when checking main-speaker conflicts), so the same
    # "proceeding" person will have multiple contexts.
    people_dict = dict( (remove_accents(person['label']), deepcopy(person))
                        for p in proceedingitems
                        for person in mediaitem['people'] + p['people'] )

    # Copy back attributes from media if necessary - they may have
    # been overwritten (in the general case)
    if mediaitem['people']:
        media_person = mediaitem['people'][0]
        person = people_dict[remove_accents(media_person['label'])]
        if media_person.get('role'):
            person['role'] = media_person['role']
        person['context'] = media_person['context']

    output['people'] = list(people_dict.values())

    # Compute a confidence score:
    # - if both main speaker and title match, then assume 1
    # - if main speaker does not match, * .5
    # - if title does not match, * .9
    confidence = 1

    # One last check - we should have a main-speaker as first
    # person. And if the second person also has main-speaker info, it
    # means that this info comes from proceedings, in which case we
    # fix it to main-proceedings-speaker
    if output['people']:
        first_person = output['people'][0]
        if first_person['context'] != 'main-speaker':
            logger.error(f"Error in {mediaitem['session']['number']}: first person ({first_person['label']}) should alway be main-speaker")
            # Bail out with no info.
            return []
        if len(output['people']) > 1:
            second_person = output['people'][1]
            if second_person['context'] == 'main-speaker':
                # We have a mismatch in main speaker definition btw
                # media and proceedings. Add a specific status to mark
                # it.
                second_person['context'] = 'main-proceeding-speaker'
                confidence *= .5
            for person in output['people'][2:]:
                # If many proceedings were merged, there may be
                # multiple other main-speaker. Give them the "speaker"
                # status.
                if person['context'] == 'main-speaker':
                    person['context'] = 'speaker'

    # Merge textContents from all proceeedings
    output['textContents'] = [ tc
                               for p in proceedingitems
                               for tc in p['textContents'] ]
    output['documents'] = [ doc
                            for p in proceedingitems
                            for doc in p['documents'] ]

    output['debug']['confidence'] = confidence
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
            {
                "speech_index": item['speechIndex'],
                "speaker": speaker_cleanup(item, "NO_SPEAKER"),
                "title": item['agendaItem']['officialTitle'],
                "item": item
             }
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

    # Build the score matrix - start at 1 since 0 row/col has no ancestor
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
        diagonal = scores[i - 1][j - 1]
        up = scores[i][j - 1]
        left = scores[i - 1][j]
        if diagonal >= up and diagonal >= left:
            i = i - 1
            j = j - 1
        elif left >= up:
            i = i - 1
        else:
            j = j - 1

    # Either i = 0 or j = 0 - add last steps to origin to make sure we
    # reach first media.

    # If we do not have i == 0, it means that we reached the beginning
    # of proceedings first. It often happens if Eröffnung is skipped
    # in the proceedings (eg 19001), or if it is split between
    # multiple speakers (eg 20021)

    # In this case, we should add mutiple steps to reach first media,
    # associating it as a best guess with the same proceeding.
    while i >= 0:
        path.append({ "media_index": i,
                      "proceeding_index": j,
                      "score": max_score,
                      "media": media_index[i]['item'],
                      "proceeding": proceedings_index[j]['item'],
                     })
        i = i - 1

    # Reverse the path, so that is in ascending order
    path.reverse()

    return path

def is_utc_offset(s: str) -> bool:
    return re.match('^[+-]\d\d:\d\d$', s)

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

    # Add linkedMediaIndexes info - it indicates the cases where the
    # same proceeding has been linked with multiple media items.

    # For this case to be properly handled, we should split the
    # proceedings in the media (through speech recognition and text
    # alignment).
    proceeding2media = {}
    for speech in speeches:
        mid = speech['debug']['mediaIndex']
        for pi in speech['debug']['proceedingIndexes']:
            proceeding2media.setdefault(pi, set()).add(mid)
    # Now that we have built the index, put the info in each speech
    for speech in speeches:
        mid = speech['debug']['mediaIndex']
        linkedMediaIndexes = list(set(mid
                                      for pid in speech['debug']['proceedingIndexes']
                                      for mid in proceeding2media[pid]))
        speech['debug']['linkedMediaIndexes'] = linkedMediaIndexes

    # Let's fix dateStart/dateEnd: the official info is in proceedings
    # (sitzung-start/ende-uhrzeit), but the UTC offset is only defined
    # in media timestamps.
    utc_offset = media['meta']['dateStart'][-6:]
    # Check that we actually have a UTC offset
    if is_utc_offset(utc_offset) and not is_utc_offset(proceedings['meta']['dateStart'][-6:]):
        # Simply copy the UTC offset string at the end.
        dateStart = proceedings['meta']['dateStart'] + utc_offset
        dateEnd = proceedings['meta']['dateEnd'] + utc_offset

    # Update session info in all speeches
    for speech in speeches:
        speech['session']['dateStart'] = dateStart
        speech['session']['dateEnd'] = dateEnd

    return { "meta": { **proceedings['meta'],
                       "dateStart": dateStart,
                       "dateEnd": dateEnd,
                       "processing": {
                           **proceedings['meta'].get('processing', {}),
                           **media['meta'].get('processing', {}),
                           "merge": datetime.now().isoformat('T', 'seconds'),
                       },
                      },
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
        media['meta']['processing']['merge'] = datetime.now().isoformat('T', 'seconds'),
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

    p = Path(args.proceedings_file)
    m = Path(args.media_file)

    output = merge_files(p, m, args)
    if args.output:
        d = Path(args.output) / f"{output['meta']['session']}-merged.json"
        out = open(d, 'w')
    else:
        out = sys.stdout
    json.dump(output, out, indent=2, ensure_ascii=False)

