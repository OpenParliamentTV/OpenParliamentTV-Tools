#! /usr/bin/env python3

# Extract entities from proceedings text

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
from pathlib import Path
import requests.exceptions
import spacy
import sys
import time

# Allow relative imports if invoked as a script
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name


def extract_entities(source: list, args) -> list:
    """Extract entities from source file

    It uses the args.lang parameter to specify the language
    """
    # nlp = spacy.blank(args.lang)
    nlp = spacy.load(f"{args.lang}_core_news_md")
    if 'entityfishing' in nlp.factory_names:
        nlp.add_pipe("entityfishing", config={ 'language': args.lang,
                                               'api_ef_base': "<api-endpoint>" })
    else:
        logger.error("Cannot find entityfishing spaCy factory. Cannot do NER.")
        return

    for item in source:
        start_time = time.time()
        for content in item.get('textContents', []):
            for speech in content.get('textBody', []):
                for sentence in speech.get('sentences', []):
                    try:
                        doc = nlp(sentence.get('text', ""))
                        entities = [ dict(label=ent.text,
                                          wid=ent._.kb_qid,
                                          wtype=ent.label_,
                                          score=ent._.nerd_score)
                                     for ent in doc.ents ]
                        sentence['entities'] = entities
                    except requests.exceptions.HTTPError as e:
                        # The entity-fishing server may respond with a 503 server error
                        logger.error(f"NER Server error: {e}")
        end_time  = time.time()
        debug = item.setdefault('debug', {})
        debug['ner-duration'] = end_time - start_time

    return source

def extract_entities_from_file(source_file, output_file, args):
    with open(source_file) as f:
        source = json.load(f)

    data = extract_entities(source['data'], args)

    output = { "meta": { **source['meta'],
                         'processing': {
                             **source['meta'].get('processing', {}),
                             "ner": datetime.now().isoformat('T', 'seconds'),
                         }
                    },
               "data": data
              }
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract entities from proceedings text in OPTV json.")
    parser.add_argument("source", type=str, nargs='?',
                        help="Source JSON file")
    parser.add_argument("output", type=str, nargs='?', default="-",
                        help="Output file")
    parser.add_argument("--lang", type=str, default="de",
                        help="Language")
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
