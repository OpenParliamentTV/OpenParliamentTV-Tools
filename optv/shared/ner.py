#! /usr/bin/env python3

# Extract entities from proceedings text

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
import requests.exceptions
import spacy
import sys
import time


def _resolve_manifest_defaults(args) -> tuple[str, str]:
    """Read ``args.spacy_model`` / ``args.entityfishing_language`` with a
    one-time deprecation path for legacy ``--lang``-only callers."""
    spacy_model = getattr(args, "spacy_model", None)
    ef_lang = getattr(args, "entityfishing_language", None)
    if spacy_model and ef_lang:
        return spacy_model, ef_lang
    legacy_lang = getattr(args, "lang", None)
    if not legacy_lang:
        raise ValueError(
            "ner.extract_entities requires args.spacy_model and "
            "args.entityfishing_language (set them in the parliament's "
            "manifest.yaml under 'locale')."
        )
    logger.warning(
        "ner.extract_entities: deriving spacy_model/entityfishing_language "
        "from legacy --lang=%r; set them via manifest.locale instead.",
        legacy_lang,
    )
    return spacy_model or f"{legacy_lang}_core_news_md", ef_lang or legacy_lang


def _build_pipeline(spacy_model: str, ef_lang: str, api_ef_base: str):
    """Load a spaCy model and attach the entityfishing pipe. Returns the
    nlp object, or None if entityfishing isn't registered (e.g. import path
    misconfigured)."""
    # Lazy import so test/parse-only code paths don't require spacyfishing.
    # Gate must use ``has_factory`` rather than ``factory_names``: if a
    # parliament's parser ran ``spacy.load(...)`` earlier in the process,
    # ``nlp.factory_names`` is stale and won't list 'entityfishing' even
    # though the factory is registered and ``add_pipe`` works.
    import spacyfishing  # noqa: F401  registers the 'entityfishing' factory
    try:
        nlp = spacy.load(spacy_model)
    except (OSError, ImportError) as e:
        logger.error(
            "spacy.load(%r) failed: %s. Install via "
            "`python -m spacy download %s`.",
            spacy_model, e, spacy_model,
        )
        return None
    if not nlp.has_factory('entityfishing'):
        logger.error("Cannot find entityfishing spaCy factory. Cannot do NER.")
        return None
    nlp.add_pipe("entityfishing", config={'language': ef_lang,
                                           'api_ef_base': api_ef_base})
    return nlp


def _run_pipeline_on(nlp, group: list):
    """Run an already-built spaCy pipeline over a list of speeches in place."""
    for item in group:
        start_time = time.time()
        for content in item.get('textContents', []):
            for speech in content.get('textBody', []):
                for sentence in speech.get('sentences', []):
                    try:
                        doc = nlp(sentence.get('text', ""))
                        entities = [dict(label=ent.text,
                                         wid=ent._.kb_qid,
                                         wtype=ent.label_,
                                         score=ent._.nerd_score)
                                    for ent in doc.ents
                                    if ent._.kb_qid]
                        sentence['entities'] = entities
                    except requests.exceptions.HTTPError as e:
                        logger.error(f"NER Server error: {e}")
        end_time = time.time()
        debug = item.setdefault('debug', {})
        debug['ner-duration'] = end_time - start_time


def extract_entities(source: list, args) -> list:
    """Extract entities from a list of speeches using the parliament's
    manifest-supplied spaCy model + entityfishing language."""
    if not args.ner_api_endpoint:
        return source

    spacy_model, ef_lang = _resolve_manifest_defaults(args)
    nlp = _build_pipeline(spacy_model, ef_lang, args.ner_api_endpoint)
    if nlp is None:
        return source
    _run_pipeline_on(nlp, source)
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
    parser.add_argument("--spacy-model", type=str, default=None,
                        help="Full spaCy model id (e.g. 'de_core_news_md', 'sv_core_news_lg'). "
                             "Required for NER. Usually sourced from manifest.locale.spacy_model.")
    parser.add_argument("--entityfishing-language", type=str, default=None,
                        help="2-letter language code for entityfishing (e.g. 'de', 'sv'). "
                             "Required for NER. Usually sourced from manifest.locale.entityfishing_language.")
    parser.add_argument("--lang", type=str, default=None,
                        help="DEPRECATED: legacy single language flag. Prefer --spacy-model "
                             "and --entityfishing-language (or manifest.locale).")
    parser.add_argument("--ner-api-endpoint", type=str, default="",
                        help="API endpoint URL for entityfishing server")
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
