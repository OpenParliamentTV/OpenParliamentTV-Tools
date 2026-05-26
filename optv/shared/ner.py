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


def _speech_language(speech: dict) -> str | None:
    """Return the speech's ISO 639-1 language code, or None if not tagged.

    Looks at the top-level ``originalLanguage`` field first (the canonical
    location, added 2026-05 for the EU multilingual integration); falls back
    to ``textContents[0].language`` if the top-level field is missing.
    Single-language parliaments (DE/SE/ES) leave both fields empty and the
    manifest defaults apply.
    """
    lang = (speech.get("originalLanguage") or "").strip()
    if lang:
        return lang
    tcs = speech.get("textContents") or []
    if tcs:
        v = (tcs[0].get("language") or "").strip()
        # Strip parliament prefix used historically (e.g. "DE-de" → "de").
        if "-" in v:
            v = v.split("-", 1)[-1]
        return v or None
    return None


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
    import spacyfishing  # noqa: F401  registers the 'entityfishing' spaCy factory
    try:
        nlp = spacy.load(spacy_model)
    except (OSError, ImportError) as e:
        logger.error(
            "spacy.load(%r) failed: %s. Install via "
            "`python -m spacy download %s`.",
            spacy_model, e, spacy_model,
        )
        return None
    if 'entityfishing' not in nlp.factory_names:
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
    """Extract entities from a list of speeches, routing per-speech language
    when present.

    Behaviour:

    * Speeches without ``originalLanguage`` / ``textContents[0].language``
      use ``args.spacy_model`` + ``args.entityfishing_language`` from the
      parliament's manifest — single-language parliaments (DE/SE/ES) hit
      this path and run a single spaCy pipeline over all speeches (the
      historical fast path; zero behaviour change).
    * Speeches with a per-speech language tag are partitioned by
      (spacy_model, entityfishing_language); each group is processed with
      its own pipeline. Unknown / low-resource languages fall back to
      ``xx_ent_wiki_sm`` + entityfishing language ``en`` (see
      :mod:`optv.shared.spacy_models`).

    The shared model registry deliberately lives outside this module so the
    same routing applies wherever ``extract_entities`` is called from.
    """
    if not args.ner_api_endpoint:
        return source

    # Resolve per-speech (spacy_model, ef_lang) targets, falling back to
    # manifest defaults for untagged speeches.
    from optv.shared.spacy_models import resolve_spacy_model, resolve_ef_language

    has_any_per_speech_lang = any(_speech_language(sp) for sp in source)
    if not has_any_per_speech_lang:
        # Single-language parliament — preserve the historical fast path.
        spacy_model, ef_lang = _resolve_manifest_defaults(args)
        nlp = _build_pipeline(spacy_model, ef_lang, args.ner_api_endpoint)
        if nlp is None:
            return source
        _run_pipeline_on(nlp, source)
        return source

    # Multilingual parliament (EU and friends) — partition by target pipeline.
    manifest_spacy, manifest_ef = (
        getattr(args, "spacy_model", None),
        getattr(args, "entityfishing_language", None),
    )
    by_pipeline: dict[tuple[str, str], list] = {}
    for sp in source:
        lang = _speech_language(sp)
        if not lang:
            if manifest_spacy and manifest_ef:
                key = (manifest_spacy, manifest_ef)
            else:
                # No per-speech lang AND no manifest default — fall back.
                key = (resolve_spacy_model(None), resolve_ef_language(None))
        else:
            key = (resolve_spacy_model(lang), resolve_ef_language(lang))
        by_pipeline.setdefault(key, []).append(sp)

    # Track which (model, ef_lang) targets resolved to xx_ent_wiki_sm fallback
    # because the native model isn't installed locally — we batch these together.
    from optv.shared.spacy_models import MULTILINGUAL_MODEL
    pending_fallback: list = []

    for (spacy_model, ef_lang), group in by_pipeline.items():
        if spacy_model == MULTILINGUAL_MODEL:
            # Already targeting the multilingual model — defer to the fallback
            # batch so we load it only once.
            pending_fallback.extend(group)
            continue
        logger.info(
            "NER pipeline %s + ef_lang=%s — %d speech(es)",
            spacy_model, ef_lang, len(group),
        )
        nlp = _build_pipeline(spacy_model, ef_lang, args.ner_api_endpoint)
        if nlp is None:
            # Native model isn't installed — fall back to xx_ent_wiki_sm with
            # entityfishing language "en". Per the manifest contract: operators
            # opt in to native models by installing them; everything else routes
            # through the multilingual fallback rather than being silently
            # skipped (which would drop ~70% of EU speeches in a typical run).
            logger.info(
                "  %s not installed; routing %d speech(es) to %s fallback",
                spacy_model, len(group), MULTILINGUAL_MODEL,
            )
            pending_fallback.extend(group)
            continue
        _run_pipeline_on(nlp, group)

    if pending_fallback:
        logger.info(
            "NER pipeline %s + ef_lang=en — %d speech(es) (multilingual fallback)",
            MULTILINGUAL_MODEL, len(pending_fallback),
        )
        nlp = _build_pipeline(MULTILINGUAL_MODEL, "en", args.ner_api_endpoint)
        if nlp is None:
            logger.error(
                "%s couldn't be loaded — %d speech(es) will be left without NER. "
                "Install via `python -m spacy download %s`.",
                MULTILINGUAL_MODEL, len(pending_fallback), MULTILINGUAL_MODEL,
            )
        else:
            _run_pipeline_on(nlp, pending_fallback)

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
