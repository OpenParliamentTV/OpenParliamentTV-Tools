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
import sys

# Allow relative imports if invoked as a script
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

def link_entities(source: list, persons: dict, factions: dict) -> list:
    """Link entities from source file
    """
    for speech in source:
        for p in speech['people']:
            label = p['label']
            if persons.get(label):
                # Found exact match
                p['wid'] = persons[label]['id']
                p['wtype'] = 'PERSON'
            faction = p.get('faction')
            if not isinstance(faction, dict) and factions.get(faction):
                f = factions[faction]
                p['faction'] = {
                    'wid': f['id'],
                    'label': faction,
                    'wtype': 'ORG'
                }
    return source

def link_entities_from_file(source_file: Path,
                            output_file: Path,
                            person_file: Path = None,
                            faction_file: Path = None):
    with open(source_file) as f:
        source = json.load(f)

    persons = {}
    factions = {}
    if person_file:
        with open(person_file) as f:
            # Convert to a dict for basic lookup
            for p in json.load(f):
                persons[p['label']] = p
                for l in p.get('altLabel', []):
                    persons[l] = p

    if faction_file:
        with open(faction_file) as f:
            factions = dict( (p.get('labelAlternative', p.get('label')), p) for p in json.load(f) )

    data = link_entities(source['data'], persons, factions)

    output = { "meta": { **source['meta'],
                         "lastUpdate": datetime.now().isoformat('T', 'seconds'),
                         "lastProcessing": "nel" },
               "data": data }
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Link Named Entities from session file.")
    parser.add_argument("source", type=str, nargs='?',
                        help="Source JSON file")
    parser.add_argument("output", type=str, nargs='?', default="-",
                        help="Output file")
    parser.add_argument("--person-data", action="store",
                        default=None,
                        help="Path to person.json file")
    parser.add_argument("--faction-data", action="store",
                        default=None,
                        help="Path to faction.json file")
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

    if not args.person_data and not args.faction_data:
        # No data specified, nothing to do
        logger.error("No reference data for persons or factions, bailing out.")
        sys.exit(1)

    link_entities_from_file(Path(args.source),
                            Path(args.output),
                            Path(args.person_data),
                            Path(args.faction_data))
