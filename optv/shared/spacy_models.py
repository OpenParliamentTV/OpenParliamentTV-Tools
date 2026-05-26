"""Per-language spaCy + entityfishing model registry.

Used by ``optv.shared.ner.extract_entities`` to route NER per-speech when the
input has per-speech language tags (e.g. EU plenary speeches preserved in the
speaker's original language). Parliaments that publish a single language per
chamber (DE/SE/ES) bypass this routing entirely — their manifest defaults
continue to apply.
"""

# ISO 639-1 → spaCy pipeline name. Medium-size models where available; small
# where not (Polish, Swedish, etc.). All are downloadable via
# ``python -m spacy download <name>``.
SPACY_MODEL_BY_LANG = {
    "en": "en_core_web_md",
    "de": "de_core_news_md",
    "fr": "fr_core_news_md",
    "es": "es_core_news_md",
    "it": "it_core_news_md",
    "nl": "nl_core_news_md",
    "pt": "pt_core_news_md",
    "pl": "pl_core_news_md",
    "el": "el_core_news_md",
    "lt": "lt_core_news_md",
    "nb": "nb_core_news_md",
    "ro": "ro_core_news_md",
    "ca": "ca_core_news_md",
    "sv": "sv_core_news_md",
    "da": "da_core_news_md",
    "fi": "fi_core_news_md",
    "hr": "hr_core_news_md",
    "sl": "sl_core_news_md",
    "uk": "uk_core_news_md",
}

# spaCy's multilingual NER pipeline. Smaller-quality entity tagging than the
# per-language models, but covers any UTF-8 input.
MULTILINGUAL_MODEL = "xx_ent_wiki_sm"

# entityfishing's primary KB-disambiguation languages. Other 2-letter codes
# can still be sent (the service falls back to its multilingual mode); for
# safety we map everything outside this set to "en" which entityfishing
# treats as the canonical English Wikidata KB.
ENTITYFISHING_PRIMARY_LANGS = {"en", "de", "fr", "es", "it"}


def resolve_spacy_model(lang_iso1: str | None) -> str:
    """Return the spaCy pipeline name for an ISO 639-1 language code.

    Unknown / empty codes fall back to the multilingual NER pipeline so the
    caller never has to special-case missing language tags.
    """
    if not lang_iso1:
        return MULTILINGUAL_MODEL
    return SPACY_MODEL_BY_LANG.get(lang_iso1.lower(), MULTILINGUAL_MODEL)


def resolve_ef_language(lang_iso1: str | None) -> str:
    """Return the entityfishing 2-letter language code for an ISO 639-1 input."""
    if not lang_iso1:
        return "en"
    code = lang_iso1.lower()
    return code if code in ENTITYFISHING_PRIMARY_LANGS else "en"
