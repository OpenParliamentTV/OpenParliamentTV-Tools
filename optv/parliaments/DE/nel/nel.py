#! /usr/bin/env python3

# Do Named Entity Linking for "structural" entities (people, factions)
# in data files.

# It can do "in-place" file enhancing (if you provide the same input
# and output filenames)

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sys
import unicodedata

# Allow relative imports if invoked as a script
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

def cleanup(name):
    if not name or isinstance(name, dict):
        return None
    else:
        # Replace non-alphanumeric chars with space
        name = re.sub('[^A-Za-z]+', ' ', name)
        # Replace multiple whitespaces
        name = re.sub(r'\s+', ' ', name)
        return remove_accents(name.strip().lower())

def link_entities(source: list, persons: dict, factions: dict) -> list:
    """Link entities from source file
    """
    for speech in source:
        for p in speech.get('people', []):
            label = cleanup(p['label'])
            if persons.get(label):
                # Found exact match
                p['wid'] = persons[label]['id']
                p['wtype'] = 'PERSON'
            faction = p.get('faction')
            if faction is not None:
                if not isinstance(faction, dict):
                    # Set a default value wid = '' for elements with non-aligned labels
                    f = factions.get(cleanup(faction), { 'id': '' })
                    p['faction'] = {
                        'wid': f['id'],
                        'label': faction,
                        'wtype': 'ORG'
                    }
                # Maybe already a dict, but not yet linked. Try again
                elif not faction.get('wid'):
                    f = factions.get(cleanup(faction['label']), { 'id': '' })
                    faction['wid'] = f['id']
    return source

def get_nel_data(nel_data_dir: Path = None):
    nel_data_file = nel_data_dir / "entities.json"

    persons = {}
    factions = {}

    if nel_data_file and nel_data_file.is_file():
        with open(nel_data_file) as f:
            nel_data = json.load(f)
        # Convert to a dict for basic lookup
        for ent in nel_data['data']:
            if ent['type'] == 'person':
                store = persons
            elif ent['type'] == 'organisation':
                store = factions
            else:
                continue
            store[cleanup(ent['label'])] = ent
            for alt in ent['labelAlternative']:
                store[cleanup(alt)] = ent
    else:
        logger.error(f"Cannot read entities from {nel_data_file}")
    return persons, factions

def link_entities_from_file(source_file: Path,
                            output_file: Path,
                            persons: dict,
                            factions: dict):
    with open(source_file) as f:
        source = json.load(f)

    data = link_entities(source['data'], persons, factions)

    output = { "meta": { **source['meta'],
                         'processing': {
                             **source['meta']['processing'],
                             "nel": datetime.now().isoformat('T', 'seconds'),
                         }
                         },
               "data": data }
    logger.info(f"Writing {output_file.name}")
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

def link_entities_from_directory(source_dir: Path,
                                 persons: dict,
                                 factions: dict):
    for source in sorted(source_dir.glob('*.json')):
        link_entities_from_file(source, source, persons, factions)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Link Named Entities from session file.")
    parser.add_argument("source", type=str, nargs='?',
                        help="Source JSON file")
    parser.add_argument("output", type=str, nargs='?', default="-",
                        help="Output file")
    parser.add_argument("--nel-data-dir", action="store",
                        default=None,
                        help="Path to NEL data dir")
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

    if not args.nel_data_dir:
        # No data specified, nothing to do
        logger.error("No data dir for entities -- specify --nel-data-dir option.")
        sys.exit(1)

    persons, factions = get_nel_data(Path(args.nel_data_dir))

    source = Path(args.source)
    output = Path(args.output)
    if source.is_dir():
        link_entities_from_directory(source, persons, factions)
    else:
        link_entities_from_file(source,
                                output,
                                persons,
                                factions)
