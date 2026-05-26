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

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

def cleanup(name):
    if not name or isinstance(name, dict):
        return None
    else:
        name = remove_accents(name.strip()).lower()
        # Replace non-alphanumeric chars with space
        name = re.sub('[^A-Za-z0-9]+', ' ', name)
        # Replace multiple whitespaces
        name = re.sub(r'\s+', ' ', name)
        return name

def _build_ep_id_index(persons: dict) -> dict:
    """Build {epId: entity} from the persons map.

    ``persons`` is keyed by cleaned-name strings, with duplicate values for
    each entity (one per alternative label). We dedupe by ``id(ent)``.
    """
    by_ep_id: dict = {}
    seen: set = set()
    for ent in persons.values():
        if id(ent) in seen:
            continue
        seen.add(id(ent))
        ep_id = (ent.get('additionalInformation') or {}).get('epId')
        if ep_id:
            by_ep_id[str(ep_id)] = ent
    return by_ep_id


def link_entities(source: list, persons: dict, factions: dict) -> list:
    """Link entities from source file.

    Speakers carrying a parliament-supplied person identifier in
    ``additionalInformation.epId`` are matched directly against the entity
    dump (the EU pipeline populates this from the EP Open Data API's person
    refs). If no epId match is found, fall back to the historic
    cleaned-label lookup.
    """
    persons_by_ep_id = _build_ep_id_index(persons)
    for speech in source:
        for p in speech.get('people', []):
            if not p.get('wid'):
                ep_id = (p.get('additionalInformation') or {}).get('epId')
                ent = persons_by_ep_id.get(str(ep_id)) if ep_id else None
                if ent and ent.get('id'):
                    p['wid'] = ent['id']
                    p['wtype'] = 'PERSON'
            label = cleanup(p['label'])
            if persons.get(label) and not p.get('wid'):
                # Found exact match - only fill in if no upstream wid (e.g. ParlaMint Q-IDs)
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
                # Maybe already a dict. Only fill wid if missing (preserve upstream IDs)
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
        # Convert to a dict for basic lookup.
        # Persons are loaded in two passes: `memberOfParliament` first, then
        # `person`. The `person` subType covers non-MP speakers (Bundespräsident,
        # state ministers, guest heads of state) but is also used by the platform
        # for people detected by NER in fulltext, so `memberOfParliament` entries
        # must win any cleaned-label collision -- a `person` entry only fills a
        # key no MP already claims.
        for ent in nel_data['data']:
            if ent['subType'] != 'memberOfParliament':
                continue
            persons[cleanup(ent['label'])] = ent
            for alt in ent['labelAlternative']:
                persons[cleanup(alt)] = ent
        for ent in nel_data['data']:
            if ent['subType'] != 'person':
                continue
            for label in [ent['label'], *ent['labelAlternative']]:
                key = cleanup(label)
                if key not in persons:
                    persons[key] = ent
        for ent in nel_data['data']:
            if ent['subType'] != 'faction':
                continue
            factions[cleanup(ent['label'])] = ent
            for alt in ent['labelAlternative']:
                factions[cleanup(alt)] = ent
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

    # Skip the rewrite if the existing output is already up-to-date.
    # Otherwise every NEL run bumps meta.processing.nel on every session
    # file even when no entities changed, producing 5-minute-cadence
    # timestamp-only commits on the downstream Data repo and merge
    # conflicts between the legacy cron and the Conductor.
    if output_file.exists():
        try:
            with open(output_file) as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            existing = None
        if (existing is not None
                and existing.get('data') == data
                and 'nel' in existing.get('meta', {}).get('processing', {})):
            logger.debug(f"No NEL changes for {output_file.name}, skipping write")
            return

    output = { "meta": { **source['meta'],
                         'processing': {
                             **source['meta'].get('processing', {}),
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
