#! /usr/bin/env python3

# Extract entities from proceedings text

import logging
logger = logging.getLogger(__name__)

import argparse
import json
from pathlib import Path
from spacy.lang.de import German
import sys
import time

# Allow relative imports if invoked as a script
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

# Global language model - to save load time
nlp = German()
if 'opentapioca' in nlp.factory_names:
    nlp.add_pipe("opentapioca")
else:
    logger.error("Cannot find opentapioca spaCy factory. Cannot do NER.")
    sys.exit(1)

def extract_entities(source: list, args) -> list:
    """Extract entities from source file
    """
    start_time = time.time()
    for item in source:
        for content in item.get('textContents', []):
            for speech in content.get('textBody', []):
                for sentence in speech.get('sentences', []):
                    doc = nlp(sentence.get('text', ""))
                    entities = [ dict(text=span.text,
                                      wid=span.kb_id_,
                                      label=span.label_,
                                      description=span._.description,
                                      score=span._.score)
                                 for span in doc.ents ]
                    sentence['entities'] = entities
    end_time  = time.time()

    debug = speech.setdefault('debug', {})
    debug['ner-duration'] = end_time - start_time

    return source

def extract_entities_from_file(source_file, output_file, args):
    with open(source_file) as f:
        source = json.load(f)

    output = extract_entities(source, args)

    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract entities from proceedings text in OPTV json.")
    parser.add_argument("source", type=str, nargs='?',
                        help="Source JSON file")
    parser.add_argument("output", type=str, nargs='?', default="-",
                        help="Output file")
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

    extract_entities_from_file(args.source, args.output, args)
