"""Non-destructive publish helpers shared across parliament workflows.

These keep ``processed/<session>-session.json`` monotonic when stages run
on different machines, in different order, or with a stale Tools checkout:

* ``is_demotion`` refuses to overwrite a richer published file with a thinner one.
* ``carry_forward_wids`` and ``carry_forward_enrichments`` fill missing
  per-speech enrichment fields from the published copy -- a publish can
  only ever add wids / agendaItem types / debug.confidence values, never
  silently strip ones already produced by a newer worker.
* ``data_signature`` + ``save_if_changed`` skip writes whose ``data``
  payload is byte-equal to what is already on disk (mtime hygiene).
"""

import json
import logging
from hashlib import blake2b
from pathlib import Path

logger = logging.getLogger(__name__)


def richest_source(config, session: str) -> Path:
    """Richest file to re-run an in-place stage (NEL / NER / align) over.

    ``processed/`` is the published high-water mark: the demotion guard below
    keeps it at least as rich as any local stage cache, and it is the only
    state that travels between machines via git. So prefer it, and fall back to
    the freshest cache (ner → aligned → merged) only before the first publish
    exists.

    Sourcing from the cache instead let a stale media-only ``aligned`` stub
    (produced when proceedings were briefly unavailable) shadow a fully
    transcribed published session: a stage then ran over text-less input and
    the publish guard correctly refused the demoted result, so the stage
    silently no-op'd on every run while ``processed/`` already held the real
    transcript.
    """
    processed_file = config.file(session, 'processed')
    if processed_file.exists():
        return processed_file
    for stage in ('ner', 'aligned', 'merged'):
        stage_file = config.file(session, stage)
        if stage_file.exists():
            return stage_file
    return config.file(session, 'merged')


def strip_legacy_textbody_ids(data: list) -> None:
    """Drop the legacy per-paragraph ``speech_id`` from every textBody item.

    It is redundant with ``textContents[].originTextID`` (the speech's text id)
    and read by no consumer (platform, Conductor, validators). Parsers still use
    it internally to derive ``originID``; this removes it from the *published*
    output only. Mutates ``data`` in place.
    """
    for speech in data:
        for tc in speech.get("textContents") or []:
            for item in tc.get("textBody") or []:
                if isinstance(item, dict):
                    item.pop("speech_id", None)


def data_signature(data: list) -> str:
    """Return a signature (as a string) for the given data.
    """
    h = blake2b(json.dumps(data).encode('utf-8'))
    return h.hexdigest()


def text_signature(data: list) -> str:
    """Signature of just the transcript *text* in ``data``.

    Hashes every ``textContents[].textBody[].sentences[].text`` in document
    order, ignoring entities / timing / wids (which a re-run legitimately
    changes). Used by ``is_demotion`` to tell "the transcript content changed"
    (e.g. new sentence-splitting logic) apart from "the same text, re-enriched"
    -- the former is a rebuild that must be allowed to replace stale
    timing/NER, the latter must not silently drop them.
    """
    h = blake2b()
    for speech in data:
        for tc in speech.get('textContents') or []:
            for item in tc.get('textBody') or []:
                if not isinstance(item, dict):
                    continue
                for sentence in item.get('sentences') or []:
                    if isinstance(sentence, dict):
                        h.update((sentence.get('text') or '').encode('utf-8'))
                        h.update(b'\x00')
    return h.hexdigest()


def save_if_changed(data: dict, output_file: Path) -> bool:
    """Save the data into file if it is different.

    ignoring the 'meta' properties (which contain processing info).

    Returns True if the data was actually saved.
    """
    # Consider it as different by default.
    updated_content = True
    if output_file.exists():
        old_data = json.loads(output_file.read_text())
        # Compare old_data with data, without taking meta info
        # (processing info) into account.
        old_digest = data_signature(old_data['data'])
        new_digest = data_signature(data['data'])
        if old_digest == new_digest:
            # Same content - do not save
            updated_content = False

    if updated_content:
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    return updated_content


def data_has_timing(data: list) -> bool:
    """True if any speech carries time-alignment output.

    Accepts the legacy kebab key too, so the demotion guard still protects an
    already-published (not-yet-migrated) file during the camelCase transition —
    otherwise a bare re-merge would look like a non-aligned doc and overwrite it.
    """
    return any((s.get('debug') or {}).get('alignDuration')
               or (s.get('debug') or {}).get('align-duration') for s in data)


def data_has_ner(data: list) -> bool:
    """True if any speech carries named-entity-recognition output (legacy key
    accepted too — see ``data_has_timing``)."""
    return any((s.get('debug') or {}).get('nerDuration')
               or (s.get('debug') or {}).get('ner-duration') for s in data)


def data_has_text(data: list) -> bool:
    """True if any speech carries merged proceedings transcript text.

    The transcript (``textContents``) is the most valuable, hardest-to-recover
    payload — far more so than the timing/NER enrichment derived from it. A
    media-only re-merge (proceedings temporarily unavailable / unparseable that
    run) produces speeches with no ``textContents``; without this guard it would
    silently overwrite a fully-transcribed published session with a bare stub.
    This is exactly how 21074 lost its 185-speech transcript in 2026-05 before
    any demotion guard existed.
    """
    return any(s.get('textContents') for s in data)


def data_has_documents(data: list) -> bool:
    """True if any speech carries linked official documents (Drucksachen etc.).

    Document references live only in the source proceedings, are extracted at
    the parse stage and unioned onto speeches at merge — they don't depend on
    timing/NER. A machine running a stale Tools checkout (e.g. the pre-period-21
    parser that silently extracted none) or a document-less local cache would
    otherwise re-publish empty ``documents`` over a session that already has
    them; this guards that the same way data_has_timing/_ner guard enrichment.
    """
    return any(s.get('documents') for s in data)


def is_demotion(new_data: list, published_data: list, *,
                allow_text_replace: bool = False) -> bool:
    """True if publishing new_data over published_data would drop transcript
    text, alignment, NER, or document links the published file already has.

    Keeps processed/ monotonic: a bare merged file (or any less-processed file
    produced from a stale cache) must never overwrite a richer published
    session.

    ``allow_text_replace`` (set by the ``--rebuild`` mode) relaxes exactly one
    case: when the transcript *content* genuinely changed (e.g. new
    sentence-splitting logic) the published timing/NER were derived from the
    old text and are now stale, so their absence in ``new_data`` is not counted
    as a demotion -- the rebuild's later stages re-derive them over the new
    text. The present→absent text guard below is NEVER relaxed (that is the
    irreversible media-only / crash data loss case), so even a rebuild can't
    empty a published transcript.
    """
    # Present→absent transcript: always a demotion, even under rebuild.
    if data_has_text(published_data) and not data_has_text(new_data):
        return True
    # Transcript content changed (both sides have text, different text): only a
    # rebuild may replace it (and shed the now-stale timing/NER for re-derive).
    if (data_has_text(published_data) and data_has_text(new_data)
            and text_signature(published_data) != text_signature(new_data)):
        return not allow_text_replace
    # Same text (or both text-less): the original monotonic enrichment guards.
    if data_has_timing(published_data) and not data_has_timing(new_data):
        return True
    if data_has_ner(published_data) and not data_has_ner(new_data):
        return True
    if data_has_documents(published_data) and not data_has_documents(new_data):
        return True
    return False


def _speech_key(speech: dict):
    """Stable per-speech identity for cross-stage matching.

    Speech-level id is converging on ``originID``; some (older / not-yet-re-emitted)
    outputs still carry it as ``originTextID``. The rename was name-only, so the
    *value* is identical — coalescing here lets a speech written under either name
    match the same key across a re-publish. ``speechIndex`` is the final fallback.
    """
    return speech.get('originID') or speech.get('originTextID') or speech.get('speechIndex')


# Per-speech enrichment fields that are append-only across a publish.
# When the new data lacks a field the published version already has, we
# carry the published value forward instead of dropping it -- catches the
# stale-cache regression where a worker on older Tools would otherwise
# strip fields (agendaItem.type, debug.confidence, ...) a worker on
# newer Tools had already produced.
#
# Stored as (parent_key, field_name) pairs because every enrichment we
# carry today sits one level under the speech dict. Promote to dot-paths
# only if a future field actually needs deeper nesting.
_ENRICHMENT_FIELDS = (
    ('agendaItem', 'type'),
    ('agendaItem', 'nativeType'),
    ('debug', 'confidence'),
    ('debug', 'confidenceReason'),
)


def carry_forward_enrichments(new_data: list, published_data: list) -> int:
    """Fill per-speech enrichment fields from published_data into new_data
    wherever new_data lacks them, matching speeches by originTextID.

    Same append-only philosophy as carry_forward_wids: a publish can add or
    update an enrichment value, but never silently strip one already
    present in processed/. Newer code's value always wins when present;
    only missing fields are filled. Mutates new_data; returns the number
    of field-values carried.
    """
    published_by_key = {}
    for speech in published_data:
        key = _speech_key(speech)
        if key is not None:
            published_by_key[key] = speech
    carried = 0
    for speech in new_data:
        prev = published_by_key.get(_speech_key(speech))
        if not prev:
            continue
        for parent, field in _ENRICHMENT_FIELDS:
            prev_parent = prev.get(parent)
            new_parent = speech.get(parent)
            if not isinstance(prev_parent, dict) or not isinstance(new_parent, dict):
                continue
            if field in prev_parent and field not in new_parent:
                new_parent[field] = prev_parent[field]
                carried += 1
    return carried


def carry_forward_wids(new_data: list, published_data: list) -> int:
    """Copy already-published person/faction wids into new_data wherever it
    lacks them, matching speeches by originTextID and people by label.

    Makes entity links append-only across a publish: a publish can add wids
    but never remove one processed/ already has, even when fed by an
    out-of-date cache. Mutates new_data; returns the number of wids carried.
    """
    published_by_key = {}
    for speech in published_data:
        key = _speech_key(speech)
        if key is not None:
            published_by_key[key] = speech
    carried = 0
    for speech in new_data:
        prev = published_by_key.get(_speech_key(speech))
        if not prev:
            continue
        prev_people = {p['label']: p
                       for p in (prev.get('people') or [])
                       if p.get('label') and p.get('wid')}
        for person in (speech.get('people') or []):
            ref = prev_people.get(person.get('label'))
            if not ref:
                continue
            if not person.get('wid') and ref.get('wid'):
                person['wid'] = ref['wid']
                if ref.get('wtype'):
                    person['wtype'] = ref['wtype']
                carried += 1
            faction, ref_faction = person.get('faction'), ref.get('faction')
            if (isinstance(faction, dict) and isinstance(ref_faction, dict)
                    and not faction.get('wid') and ref_faction.get('wid')):
                faction['wid'] = ref_faction['wid']
                if ref_faction.get('wtype'):
                    faction['wtype'] = ref_faction['wtype']
    return carried


def carry_forward_documents(new_data: list, published_data: list) -> int:
    """Fill a speech's ``documents`` from published_data wherever new_data has
    none, matching speeches by originTextID.

    Append-only, like carry_forward_wids: a publish can add or replace document
    links, but a speech that already has documents in processed/ must not be
    silently emptied by a worker on a stale Tools checkout / document-less local
    cache. Newer data wins when it carries any documents of its own; only a
    speech with an empty (or absent) list is filled. Mutates new_data; returns
    the number of speeches filled.
    """
    published_by_key = {}
    for speech in published_data:
        key = _speech_key(speech)
        if key is not None:
            published_by_key[key] = speech
    carried = 0
    for speech in new_data:
        if speech.get('documents'):
            continue
        prev = published_by_key.get(_speech_key(speech))
        if prev and prev.get('documents'):
            speech['documents'] = prev['documents']
            carried += 1
    return carried
