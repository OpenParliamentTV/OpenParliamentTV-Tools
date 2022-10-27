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
import re
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
    try:
        output['agendaItem']['proceedingIndex'] = first_proceeding['agendaItem']['speechIndex']
    except KeyError:
        import pdb; pdb.set_trace()
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

def merge_item_old(proceeding, mediaitem):
    # Non-matching case - return the unmodified value
    if proceeding is None:
        mediaitem['agendaItem']['proceedingIndex'] = None
        mediaitem['agendaItem']['mediaIndex'] = mediaitem['agendaItem']['speechIndex']
        return mediaitem
    if mediaitem is None:
        proceeding['agendaItem']['proceedingIndex'] = proceeding['agendaItem']['speechIndex']
        proceeding['agendaItem']['mediaIndex'] = None
        return proceeding

    # We have both items - copy media data into proceedings
    # Make a copy of the proceedings file
    output = deepcopy(proceeding)

    # Copy relevant data from mediaitem
    output['agendaItem']['title'] = mediaitem['agendaItem']['title']
    output['agendaItem']['proceedingIndex'] = proceeding['agendaItem']['speechIndex']
    output['agendaItem']['mediaIndex'] = mediaitem['agendaItem']['speechIndex']
    output['dateStart'] = mediaitem['dateStart']
    output['dateEnd'] = mediaitem['dateEnd']
    output['media'] = mediaitem['media']

    return output

def speaker_cleanup(item):
    if item.get('people'):
        # Warning: we use people[0] assuming it is the main
        # speaker. It works because proceedings2json (now) explicitly
        # sorts the people list
        speaker = remove_accents(item['people'][0]['label'].lower()).replace(' von der ', ' ').replace('altersprasident ', '')
    else:
        speaker = None
    return speaker

def get_item_key(item):
    speaker = speaker_cleanup(item)
    title = item['agendaItem']['officialTitle'].strip()

    # Remove non-breaking spaces (interim - this is done in parsers/common)
    title = re.sub(r'\xc2\xa0', ' ', title)

    # Remove trailing .<number>
    title = re.sub('\.\d+$', '', title)
    # For 20024 media files:
    title = re.sub('Einzelplan 0', 'Einzelplan ', title)
    # For 20036
    title = re.sub(' TOP ', ' ', title)

    # Tentative (for 20036, 20043...): strip TOP number
    # Do not use: it worsens the situation in many cases like 19206, 19213
    # title = re.sub('Tagesordnungspunkt\s(\d+)', 'Tagesordnungspunkt', title)

    # Replace MM-NN by only the 1st item (ideally we should generate a sequence MM..NN)
    # title = re.sub('\s(\d+)-\d+$', ' \\1', title)
    return remove_accents(f"{item['electoralPeriod']['number']}-{item['session']['number']} {title} ({speaker})".lower())

def bounded_non_matching_sequences(mapping_sequence):
    """Takes a (proceeding, media) sequence

    Yields sub-sequences of empty proceedings with non-empty
    proceeding boundaries
    """
    def groupkey(tup):
        return "MATCH" if tup[0] is not None else "UNMATCH"

    return itertools.groupby(mapping_sequence, groupkey)

def align_nonmatching_subsequences(mapping_sequence, proceedings, media, options):
    # Mapping_sequence is a list of (proceeding, media) tuples

    # Some of the "proceeding" values may be None, when we could not align them with the whole key.

    # Other option: see https://pypi.org/project/alignment/ for alignment of sub-sequences

    # Categorize sequence. Output a list of [ ("MATCH", [ (p1, m1), (p2, m2), ... ]),
    #                                         ("UNMATCH", [ (None, m5), (None, m6)... ]), ... ]
    categorized_sequences = [ (k, list(seq))
                               for (k, seq) in bounded_non_matching_sequences(mapping_sequence)
                              ]
    #for (k, seq) in non_matching_sequences:
    #    print(f"""{k} - {len(seq)} items""")
    for i, group in enumerate(categorized_sequences):
        category, sequence = group
        if category == 'UNMATCH':
            # We have a sequence with tup[0] (proceeding) == None.

            # Extract from global proceedings list the sequence
            # between the previous matching proc. item and the next matching proc. item
            proc_sequence = list(proceedings)
            if i > 0:
                prev_match = categorized_sequences[i - 1]
                assert prev_match[0] == 'MATCH'
                prev_proc = prev_match[1][-1][0]
                proc_sequence = itertools.dropwhile(lambda p: p['key'] != prev_proc['key'],
                                                    proc_sequence)
            if i < len(categorized_sequences) - 1:
                next_match = categorized_sequences[i + 1]
                assert next_match[0] == 'MATCH'
                next_proc = next_match[1][0][0]
                proc_sequence = itertools.takewhile(lambda p: p['key'] != next_proc['key'],
                                                    proc_sequence)
            # We should now have a corresponding proceedings sequence that we must align
            proc_sequence = list(proc_sequence)
            logger.debug(f"--- {len(proc_sequence)} / {len(sequence)} non matching items -----")
            if options.debug:
                for m, p in itertools.zip_longest(sequence, proc_sequence):
                    try:
                        logger.debug("%s\t%s" % (m[1]['people'][0]['label'] if (m and m[1].get('people')) else 'None',
                                                 p['people'][0]['label'] if (p and p.get('people')) else 'None'))
                    except (IndexError, KeyError):
                        logger.debug("Exception in merging media {m['key']} and proceeding {p['key']} - missing info")
            # Now align items
            for p, m in sequence:
                # p is None since we are in an UNMATCH group
                key = speaker_cleanup(m)
                # Try to find a matching name in proc_sequence
                matching_proc = None
                if proc_sequence:
                    p = proc_sequence[0]
                    if speaker_cleanup(p) == key:
                        # Matching speaker name
                        matching_proc = proc_sequence.pop(0)
                    elif options.advanced_rematch and len(proc_sequence) > 1:
                        # Try one item further
                        p = proc_sequence[1]
                        if speaker_cleanup(p) == key:
                            # Matching speaker name
                            matching_proc = p
                            # Remove 2 items
                            proc_sequence.pop(0)
                            proc_sequence.pop(0)

                yield matching_proc, m
        else:
            for tup in sequence:
                yield tup

def matching_items(proceedings, media, options):
    """Return a list of (proceeding, mediaitem) items that match.
    """
    # Build a dict for proceedings, indexed by key
    procdict = {}
    mediadict = {}
    for label, source, itemdict in ( ('proceedings', proceedings, procdict),
                                     ('media', media, mediadict) ):
        for item in source:
            # Get standard key
            item['key'] = get_item_key(item)
            if item['key'] in itemdict:
                # Duplicate key - add a #N to the key to differenciate
                # We do not use item['agendaItem']['speechIndex']
                # because we want to use the relative appearing order of items.
                n = 1
                while True:
                    newkey = f"{item['key']} #{n}"
                    if newkey not in itemdict:
                        break
                    n = n + 1
                item['key'] = newkey
            itemdict[item['key']] = item

    # Determine all key-based matching items
    output = [ (procdict.get(m['key']), m) for m in media ]
    # FIXME: add a "matching: primary-key" info to indicate source of matching

    if options and (options.second_stage_matching or options.advanced_rematch):
        # Using matching items as landmarks, try to align remaining
        # sequences based on speaker names matching
        output = list(align_nonmatching_subsequences(output, proceedings, media, options))

    output_proceeding_keys = set( p['key']
                                  for p, m in output
                                  if p is not None )

    if options and options.include_all_proceedings:
        # Add proceeding items with no matching media items - in speechIndex order
        proc_items = ( p for p in proceedings if p['key'] not in output_proceeding_keys )
        output.extend((item, None) for item in proc_items)
    return output

def diff_files(proceedings_file, media_file, options):
    with open(proceedings_file) as f:
        proceedings = json.load(f)
    with open(media_file) as f:
        media = json.load(f)
    #width = int(int(os.environ.get('COLUMNS', 80)) / 2)
    width = 60
    left = "Proceeding"
    right = "Media"
    print(f"""{left.ljust(width)} {right}""")
    for (p, m) in matching_items(proceedings, media, options):
        left = '[[[ None ]]]' if p is None else p['key']
        right = '[[[ None ]]]' if m is None else m['key']
        print(f"""{left.ljust(width)} {right}""")

def unmatched_count(proceedings_file, media_file, options=None):
    try:
        with open(proceedings_file) as f:
            proceedings = json.load(f)
    except FileNotFoundError:
        proceedings = []
    try:
        with open(media_file) as f:
            media = json.load(f)
    except FileNotFoundError:
        media = []

    matching = matching_items(proceedings, media, options)
    unmatched_proceedings = [ p for (p, m) in matching if m is None ]
    unmatched_media = [ m for (p, m) in matching if p is None ]
    return {
        'proceedings_file': str(proceedings_file),
        'media_file': str(media_file),
        'proceedings_count': len(proceedings),
        'media_count': len(media),
        'unmatched_proceedings_count': len(unmatched_proceedings),
        'unmatched_media_count': len(unmatched_media)
    }

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

    # Similarity score between 2 items
    def similarity(m, p):
        # FIXME: use string distance (Levensteihn?) rather than strict equality?
        return config['speaker_weight'] * int(m['speaker'] == p['speaker']) + config['title_weight'] * int(m['title'] == p['title'])

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

def merge_data_old(proceedings, media, options):
    """Merge data structures.

    If no match is found for a proceedings, we will dump the
    proceedings as-is.
    """
    return [
        merge_item_old(p, m)
        for (p, m) in matching_items(proceedings, media, options)
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

    # Use getattr to allow undefined args.unmatched_count/check
    if getattr(args, 'unmatched_count', None):
        is_first = True
        print('[')
        for (p, m) in pairs:
            if p is None:
                continue
            if not is_first:
                print(",")
            print(json.dumps(unmatched_count(p, m, args), indent=2, ensure_ascii=False))
            is_first = False
        print(']')
        sys.exit(0)
    elif getattr(args, 'check', None):
        for (p, m) in pairs:
            if p is None:
                continue
            print(f"* Difference between {p.name} and {m.name}")
            diff_files(p, m, args)
            print("\n")
        return []
    else:
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
    parser.add_argument("--check", action="store_true",
                        default=False,
                        help="Check mergeability of files")
    parser.add_argument("--unmatched-count", action="store_true",
                        default=False,
                        help="Only display the number of unmatched proceeding items")
    parser.add_argument("--include-all-proceedings", action="store_true",
                        default=False,
                        help="Include all proceedings-issued speeches even if they did not have a match")
    parser.add_argument("--second-stage-matching", action="store_true",
                        default=False,
                        help="Do a second-stage matching using speaker names for non-matching subsequences")
    parser.add_argument("--advanced-rematch", action="store_true",
                        default=False,
                        help="Try harder to realign non-matching proceeding items by skipping some of the items")

    args = parser.parse_args()
    if args.media_file is None or args.proceedings_file is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel=logging.DEBUG
    logging.basicConfig(level=loglevel)

    merge_files_or_dirs(Path(args.media_file), Path(args.proceedings_file), args.output, args)
