#! /usr/bin/env python3

# Merge proceeding and media files

# It takes as input a proceeding file/dir and a media file/dir and outputs a third one with speeches merged.

from __future__ import annotations

import logging
logger = logging.getLogger('merge_session' if __name__ == '__main__' else __name__)

import argparse
from copy import deepcopy
from datetime import datetime
import itertools
import json
from pathlib import Path
import re
import sys
import unicodedata

# Allow `python -m optv.parliaments.DE.merger.merge_session` and direct
# script invocation. The shared module lives under optv.shared.
if __package__ is None or __package__ == "":
    _module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(_module_dir.parents[3]))   # repo root → optv.shared.*
    __package__ = _module_dir.name

from optv.shared.agenda_types import annotate_agenda_item, classify_de_native
from optv.shared.speech_id import normalize_speech_originid

# Q&A agenda types — Bundestag cuts one video per ministerial Q&A block while
# proceedings have many <rede> per block. Text inflates onto one media clip.
QA_TYPES = frozenset({'qa', 'questioning_of_the_government'})
# Chair-only inter-TOP transition turns ("Ich schließe …, ich rufe TOP N
# auf, …"). Tagged by parlamint2json on DE-17 ParlaMint XML. The proceedings
# text is framing (bill enumeration etc.); not substantive speech matching
# any single media clip. See DE-17-F02 in whisper_qc/DE-17/findings.md.
CHAIR_TRANSITION_TYPES = frozenset({'procedural'})
# Bimodal len(textContents) distribution at the gate-pass tail: 1–3 legitimate,
# 60+ broken (Bettermann fingerprint). Any threshold in [4, 60] is equivalent.
TEXT_CONTENTS_CAP = 5
# chars-per-second cap — a speech whose proceedings text is far longer than its
# media clip could physically contain is a mis-merge (whole-debate / wrong text
# bound onto a short clip). German speech runs ~16 cps (p90 ~20); these run
# 100s–1000s. Only gate substantive *debate* types: procedural/opening/voting/
# election etc. legitimately carry long chair text (announcements, referral
# lists) on a short representative clip and are correct-but-truncated, not wrong.
# questioning_of_the_government IS included: the qa rule above already gates ALL
# of it on re-merge, but listing it here means "extreme cps == mis-merge" also
# holds for Q&A — so consumers of this set (e.g. the backfill) catch a Q&A dump
# like a 426-cps Befragung answer WITHOUT blanket-suppressing normal Q&A turns.
CPS_CAP = 100
CPS_CAP_TYPES = frozenset({
    'regular', 'report', 'current_affairs', 'government_declaration',
    'budget', 'briefing', 'questioning_of_the_government',
})
# The char floor is source-dependent. Whisper cross-check (see
# whisper_qc/period17_readiness.md): period-17 is ParlaMint-sourced — its
# <rede> segmentation binds whole-debate text blocks onto short chair clips, so
# even ~8k-char gate-passers are *wrong content* (sim 0.01–0.11). 18–21 is
# official Bundestag XML — high-cps there is usually correct-but-truncated chair
# text; only the >25k-char giant dumps (e.g. the Bettermann tail-accumulation
# bug) are genuinely wrong. Keying on period 17 == the ParlaMint source.
CPS_CAP_CHARS_BY_PERIOD = {17: 8000}
CPS_CAP_CHARS_DEFAULT = 25000


def _text_char_count(text_contents) -> int:
    """Total characters across all sentences (mirrors the cps audit metric)."""
    return sum(len(sent.get('text') or '')
               for tc in text_contents or []
               for tb in tc.get('textBody') or []
               for sent in tb.get('sentences') or [])

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])


def _split_first_last(label: str) -> tuple[str, str]:
    """Split a "First [Middle ...] Last" label at the last space.
    Returns ('', '') if the label has no space."""
    if not label:
        return '', ''
    parts = label.rsplit(' ', 1)
    if len(parts) != 2:
        return '', ''
    return parts[0], parts[1]


# Name particles (lowercased, period-stripped). When the delta between
# long_fn and short_fn consists of — or includes — any of these, the long
# form carries a meaningful name component (German/Dutch/French/Iberian
# nobility or compound surname particles) and the "shorter wins" rule
# would silently drop part of the proper name. Observed false-positive
# class in DE 18-21: Hans-Georg von der Marwitz, Jan van Aken, Konstantin
# von Notz, Thomas de Maizière, Ursula von der Leyen, Kees de Vries,
# Matern von Marschall. Block these.
_NAME_PARTICLES = frozenset({
    # German
    'von', 'vom', 'der', 'den', 'zu', 'zur', 'zum',
    # Dutch
    'van',
    # French / Iberian / Italian
    'de', 'du', 'des', 'da', 'do', 'dos', 'das', 'di', 'della', 'la', 'le',
})


# German noble titles: the media RSS abbreviates them, the
# ParlaMint/Bundestag proceedings spell them out — same person, two labels,
# which the people de-dup would otherwise split. Surveying DE 17-21 person
# labels, only "Frhr." actually occurs (68×; "Graf" etc. appear only in
# full form). Plain literal substitution — the token is distinctive enough
# not to collide with anything else. Add entries here if a survey turns up
# more (e.g. 'Frfr.': 'Freifrau', 'Gf.': 'Graf').
_NAME_ABBREV_EXPANSIONS = {
    'Frhr.': 'Freiherr',
}


def _expand_name_abbreviations(label: str) -> str:
    """Expand abbreviated German noble titles in a person label."""
    for abbrev, full in _NAME_ABBREV_EXPANSIONS.items():
        if abbrev in label:
            label = label.replace(abbrev, full)
    return label


def _delta_tokens(short_fn: str, long_fn: str) -> list[str] | None:
    """Return the list of tokens that exist in `long_fn` but not in
    `short_fn` when `short_fn` is a separator-boundary (space or hyphen)
    prefix or suffix of `long_fn`. Returns None if the predicate doesn't
    match. Tokens are lowercased and period-stripped for particle lookup."""
    if not short_fn or not long_fn or len(short_fn) >= len(long_fn):
        return None
    delta = None
    for sep in (' ', '-'):
        if long_fn.startswith(short_fn + sep):
            delta = long_fn[len(short_fn) + 1:]
            break
        if long_fn.endswith(sep + short_fn):
            delta = long_fn[:-(len(short_fn) + 1)]
            break
    if delta is None:
        return None
    return [t.strip('.').lower() for t in re.split(r'[\s\-]+', delta) if t]


def _firstname_is_separator_variant(short_fn: str, long_fn: str) -> bool:
    """True when `short_fn` is a separator-boundary (space or hyphen) prefix
    or suffix of `long_fn` AND the delta tokens are all non-particle —
    i.e. middle initials ("E."), middle names ("David"), or honorary
    prefixes ("h.c.") rather than name particles ("von der", "van", "de").
    Inputs are pre-normalized (accent-stripped, lowercased) firstname
    strings."""
    tokens = _delta_tokens(short_fn, long_fn)
    if tokens is None:
        return False
    return not any(t in _NAME_PARTICLES for t in tokens)


def canonicalize_person_labels(people_lists, session_id: str = '') -> None:
    """Reconcile same-person label variants across media + proceedings.

    Mutates each entry of `people_lists` (a list of person-dict lists, e.g.
    `[media_people, proc1_people, proc2_people, ...]`) in place: when two
    distinct labels share the same lastname (accent-stripped, lowercased)
    and one firstname is a separator-boundary prefix/suffix of the other —
    e.g. "Hermann Ott" vs "Hermann E. Ott", "Sven Kindler" vs
    "Sven-Christian Kindler", "Jürgen Zöllner" vs "E. Jürgen Zöllner" —
    rewrite the longer-firstname label to the shorter form so the downstream
    `people_dict` de-dup collapses them into one entry, avoiding the
    speaker-mismatch confidence *= 0.5 path.

    Source of the long forms in DE-17: ParlaMint persName entries with
    multiple <forename> children that the parser concatenates verbatim.
    Sources in DE 18-21: occasional Bundestag PDF entries that include a
    middle name where the media RSS uses the short form (Wadephul,
    Neuhäuser observed).

    Failure mode (theoretical): two genuinely distinct MPs share a lastname
    and one's firstname is a separator-boundary substring of the other's
    (only known pair across DE-17 + DE 18-21: Hans-Peter Friedrich Q66144
    vs Peter Friedrich Q123872 — never co-appear in any single speech in
    1300+ sessions). Every rewrite is logged at WARN so a future collision
    is auditable.
    """
    # Expand abbreviated noble titles ("Frhr." -> "Freiherr") first, so a
    # person whose media label abbreviates and whose proceedings label
    # spells it out collapses to one entry in the downstream people_dict
    # de-dup. Otherwise the merger sees two main-speakers, drops confidence
    # to 0.5, and the media-side mention keeps no wid — e.g. Karl-Theodor
    # (Frhr.|Freiherr) zu Guttenberg in session 17093.
    session_suffix = f" (session {session_id})" if session_id else ""
    for plist in people_lists:
        for person in plist:
            label = person.get('label')
            if not label:
                continue
            expanded = _expand_name_abbreviations(label)
            if expanded != label:
                logger.info(
                    f"merge_session: expanding person label "
                    f"{label!r} -> {expanded!r}{session_suffix}"
                )
                person['label'] = expanded

    by_lastname: dict[str, dict[str, str]] = {}
    for plist in people_lists:
        for person in plist:
            label = person.get('label')
            if not label:
                continue
            fn, ln = _split_first_last(label)
            if not ln:
                continue
            ln_key = remove_accents(ln).lower()
            fn_key = remove_accents(fn).lower()
            by_lastname.setdefault(ln_key, {})[label] = fn_key

    rename: dict[str, str] = {}
    for label_map in by_lastname.values():
        if len(label_map) < 2:
            continue
        # Shortest-firstname label is the canonical candidate; only rewrite
        # others to it if they're separator-boundary variants.
        items = sorted(label_map.items(), key=lambda kv: len(kv[1]))
        canonical_label, canonical_fn = items[0]
        for label, fn in items[1:]:
            if _firstname_is_separator_variant(canonical_fn, fn):
                rename[label] = canonical_label

    if not rename:
        return

    for long_label, short_label in rename.items():
        logger.warning(
            f"merge_session: canonicalizing person label "
            f"{long_label!r} -> {short_label!r}{session_suffix}"
        )

    for plist in people_lists:
        for person in plist:
            label = person.get('label')
            if label in rename:
                person['label'] = rename[label]


def merge_item(mediaitem, proceedingitems):
    # We have both items - copy proceedings data into media item
    # Make a copy of the media data
    output = deepcopy(mediaitem)

    first_proceeding = proceedingitems[0]

    # Backward compatibility: until 2026-05-01 (parser commit f9d9ea1) the
    # speech id at the top level was emitted as `originTextID`, sharing the
    # name of the textContents-level field. Cached parser JSONs that predate
    # that rename are still valid input; accept either spelling on read.
    # New writes always emit `originID`.
    output['originID'] = first_proceeding.get('originID') or first_proceeding['originTextID']

    # Copy officialDateStart/End from proceedings
    output['session']['dateStart'] = first_proceeding['session']['dateStart']
    output['session']['dateEnd'] = first_proceeding['session']['dateEnd']

    # Copy relevant data from proceedings
    output['debug']['proceedingIndex'] = first_proceeding['speechIndex']
    output['debug']['proceedingIndexes'] = [ p['speechIndex'] for p in proceedingitems ]
    output['debug']['mediaIndex'] = mediaitem['speechIndex']
    if first_proceeding.get('debug', {}).get('proceedings-source'):
        output['debug']['proceedings-source'] = first_proceeding['debug']['proceedings-source']

    # Merge people in case of multiple proceedings. We use a dict for
    # de-duplication (instead of a set) so that we preserve order.  We
    # prepend media-based speaker info so that it always appears first
    # (and he is always tagged 'main-speaker')

    # We do a copy of person info because we will possibly update its
    # context info (when checking main-speaker conflicts), so the same
    # "proceeding" person will have multiple contexts.
    media_people = mediaitem.get('people') or []

    # Reconcile same-person label variants across media + proceedings
    # (e.g. "Hermann Ott" vs "Hermann E. Ott") before de-dup, so the
    # downstream people_dict collapses them instead of treating them as
    # two main-speakers and dropping confidence to 0.5. See helper docstring
    # for the predicate, scope, and known collision audit. In-place mutation
    # on the proceedings refs is intentional — the canonical labels then also
    # feed the session-wide wid-backfill at the bottom of merge_data().
    canonicalize_person_labels(
        [media_people] + [p.get('people') or [] for p in proceedingitems],
        session_id=str((mediaitem.get('session') or {}).get('number', '')),
    )

    people_dict = dict( (remove_accents(person['label']), deepcopy(person))
                        for p in proceedingitems
                        for person in media_people + p.get('people', []) )

    # Copy back attributes from media if necessary - they may have
    # been overwritten (in the general case)
    if media_people:
        media_person = media_people[0]
        person = people_dict[remove_accents(media_person['label'])]
        if media_person.get('role'):
            person['role'] = media_person['role']
        person['context'] = media_person['context']

    output['people'] = list(people_dict.values())

    # Compute a confidence score:
    # - if both main speaker and title match, then assume 1
    # - if main speaker does not match, * .5
    # - if title does not match, * .9
    confidence = 1

    # One last check - we should have a main-speaker as first
    # person. And if the second person also has main-speaker info, it
    # means that this info comes from proceedings, in which case we
    # fix it to main-proceedings-speaker
    # (Skip this check when media had no speaker info: the "first person
    # is main-speaker" invariant only holds when media confirmed the speaker.)
    if output['people'] and media_people:
        first_person = output['people'][0]
        if first_person['context'] != 'main-speaker':
            logger.error(f"Error in {mediaitem['session']['number']}: first person ({first_person['label']}) should alway be main-speaker")
            # Bail out with no info.
            return []
        if len(output['people']) > 1:
            second_person = output['people'][1]
            if second_person['context'] == 'main-speaker':
                # We have a mismatch in main speaker definition btw
                # media and proceedings. Add a specific status to mark
                # it.
                second_person['context'] = 'main-proceeding-speaker'
                confidence *= .5
            for person in output['people'][2:]:
                # If many proceedings were merged, there may be
                # multiple other main-speaker. Give them the "speaker"
                # status.
                if person['context'] == 'main-speaker':
                    person['context'] = 'speaker'

    # Merge textContents from all proceeedings
    output['textContents'] = [ tc
                               for p in proceedingitems
                               for tc in p['textContents'] ]
    output['documents'] = [ doc
                            for p in proceedingitems
                            for doc in p['documents'] ]

    # Agenda-type classification, three layers (annotate_agenda_item preserves
    # non-empty values, so each later step only fills gaps):
    #  1. media parser already ran classify_de_native on output.agendaItem
    #     (sticks via deepcopy(mediaitem) above)
    #  2. proceedings parser also ran classify_de_native — inherit from it
    #     when media didn't set a value (also covers period 17 / ParlaMint
    #     where the parser sets type/nativeType from the structured `ana`
    #     attribute)
    #  3. final fallback: re-classify on the merged title
    output_agenda = output.setdefault('agendaItem', {})
    proc_agenda = first_proceeding.get('agendaItem') or {}
    if proc_agenda.get('type'):
        annotate_agenda_item(output_agenda,
                             proc_agenda.get('nativeType'),
                             proc_agenda['type'])
    title = output_agenda.get('title') or output_agenda.get('officialTitle') or ''
    nt, ct = classify_de_native(title)
    annotate_agenda_item(output_agenda, nt, ct)

    # The ParlaMint parser types chair-transition turns ("(Jetzt) rufe ich
    # Tagesordnungspunkt N auf") as `procedural` from the chair <u> content
    # itself — an authoritative structural signal. But the media clip carries
    # the *next topic* as its title, which classify_de_native reads as
    # `regular`; the gap-fill annotate_agenda_item above lets that media
    # `regular` win and silently silences the chair-transition gate. Force the
    # proceedings classification to win. DE-16/17 only: DE-chair_transition is
    # emitted solely by parlamint2json (never by proceedings2json / 18-21).
    if proc_agenda.get('nativeType') == 'DE-chair_transition':
        output_agenda['type'] = 'procedural'
        output_agenda['nativeType'] = 'DE-chair_transition'

    confidence_reason = None
    agenda_type = output_agenda.get('type') or ''
    if agenda_type in QA_TYPES:
        confidence = min(confidence, 0.5)
        confidence_reason = 'qa-agenda-type'
    elif agenda_type in CHAIR_TRANSITION_TYPES:
        confidence = min(confidence, 0.5)
        confidence_reason = 'chair-transition'
    if len(output['textContents']) > TEXT_CONTENTS_CAP:
        confidence = min(confidence, 0.5)
        confidence_reason = confidence_reason or 'len-cap'
    # chars-per-second cap: text physically too long for the clip => mis-merge.
    if agenda_type in CPS_CAP_TYPES:
        duration = (output.get('media') or {}).get('duration')
        if isinstance(duration, (int, float)) and duration > 0:
            chars = _text_char_count(output['textContents'])
            period = (output.get('electoralPeriod') or {}).get('number')
            chars_floor = CPS_CAP_CHARS_BY_PERIOD.get(period, CPS_CAP_CHARS_DEFAULT)
            if chars >= chars_floor and chars / duration >= CPS_CAP:
                confidence = min(confidence, 0.5)
                confidence_reason = confidence_reason or 'cps-cap'

    output['debug']['confidence'] = confidence
    if confidence_reason:
        output['debug']['confidence_reason'] = confidence_reason
    return output

def speaker_cleanup(item, default_value):
    if item.get('people'):
        # Warning: we use people[0] assuming it is the main
        # speaker. It works because proceedings2json (now) explicitly
        # sorts the people list
        speaker = remove_accents(item['people'][0]['label'].lower()).replace(' von der ', ' ').replace('altersprasident ', '')
    else:
        speaker = default_value
    return speaker

def needleman_wunsch_align(proceedings, media, options):
    """Align data structures using Needleman-Wunsch algorithm

    DE-specific on purpose: the matrix is seeded with the substitution scores
    (no gap border), the backtrack compares raw neighbour cells, and a tail walks
    any remaining media rows onto the first proceeding (Eröffnung skipped/split,
    eg 19001/20021). These choices are tuned to the Bundestag's media↔proceedings
    structure and are not shared — only the generic equal-key form in
    ``optv.shared.sequence_align`` is.
    """
    config = {
        "speaker_weight": 4,
        "title_weight": 2,
        "merge_penalty": -1,
        "split_penalty": -1,
    }
    def build_index(items):
        return [
            {
                "speech_index": item['speechIndex'],
                "speaker": speaker_cleanup(item, "NO_SPEAKER"),
                "title": item['agendaItem']['officialTitle'],
                "item": item
             }
            for item in items
        ]
    media_index = build_index(media)
    proceedings_index = build_index(proceedings)

    # Levenshtein has been tested, but gives worse results, because
    # the differences are too small (last character for TOP)
    def string_similarity(s1, s2):
        return s1.strip() == s2.strip()

    # Similarity score between 2 items
    def similarity(m, p):
        return (config['speaker_weight'] * string_similarity(m['speaker'], p['speaker'])
                + config['title_weight'] * string_similarity(m['title'], p['title']))

    # Build the [m, p] matrix with scores using the Needleman-Wunsch algorithm
    # https://fr.wikipedia.org/wiki/Algorithme_de_Needleman-Wunsch
    # Initialize a m x p matrix
    scores = [ [ similarity(m, p) for p in proceedings_index ] for m in media_index ]
    # Or 0-initialization?
    # scores = [ [ 0 for p in proceedings_index ] for m in media_index ]

    # FIXME: maybe we could tweak merge_penalty and split_penalty based on the dissimilarity between media duration and text length.
    # A long media duration with a short text length should favor the merge option
    # Build the score matrix - start at 1 since 0 row/col has no ancestor
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
        diagonal = scores[i - 1][j - 1]
        up = scores[i][j - 1]
        left = scores[i - 1][j]
        if diagonal >= up and diagonal >= left:
            i = i - 1
            j = j - 1
        elif left >= up:
            i = i - 1
        else:
            j = j - 1

    # Either i = 0 or j = 0 - add last steps to origin to make sure we
    # reach first media.

    # If we do not have i == 0, it means that we reached the beginning
    # of proceedings first. It often happens if Eröffnung is skipped
    # in the proceedings (eg 19001), or if it is split between
    # multiple speakers (eg 20021)

    # In this case, we should add mutiple steps to reach first media,
    # associating it as a best guess with the same proceeding.
    while i >= 0:
        path.append({ "media_index": i,
                      "proceeding_index": j,
                      "score": max_score,
                      "media": media_index[i]['item'],
                      "proceeding": proceedings_index[j]['item'],
                     })
        i = i - 1

    # Reverse the path, so that is in ascending order
    path.reverse()

    return path

def is_utc_offset(s: str) -> bool:
    return re.match(r'^[+-]\d\d:\d\d$', s)

def merge_data(proceedings, media, options) -> list:
    """Merge data structures.

    If no match is found for a proceedings, we will dump the
    proceedings as-is.
    """
    path = needleman_wunsch_align(proceedings['data'], media['data'], options)

    # Drop synth chair-intro open-halves (originID '...+open', emitted by
    # parlamint2json's chair-transition split) when alignment binds them to the
    # same media as a non-procedural (MP) proceeding. That binding indicates
    # the Bundestag only published 2 media clips at this TOP boundary instead
    # of 3 (no separate chair-intro clip), so the parser's pre-emptive split
    # was over-eager: the synth would smear chair-intro text onto the first-MP
    # slot. Dropping the synth from the MP side restores the clean pre-fix
    # state; the chair side keeps both halves (still procedural → gate-failed)
    # so no chair text is lost.
    procs_by_media: dict = {}
    for entry in path:
        procs_by_media.setdefault(entry['media_index'], []).append(entry)
    drop_ids = set()
    for entries in procs_by_media.values():
        if len(entries) < 2:
            continue
        has_non_procedural = any(
            (e['proceeding'].get('agendaItem') or {}).get('type') != 'procedural'
            for e in entries
        )
        if not has_non_procedural:
            continue
        for e in entries:
            if str(e['proceeding'].get('originID') or '').endswith('+open'):
                drop_ids.add(id(e))
    if drop_ids:
        path = [e for e in path if id(e) not in drop_ids]

    # Group by media. There can be multiple proceedings
    speeches = [
        merge_item(group[0]['media'],
                   [ i['proceeding'] for i in group ])
        for group in [ list(group)
                       for media_index, group in itertools.groupby(path, lambda i: i['media_index']) ]
    ]
    # merge_item returns [] as a sentinel for "skip this item" (data inconsistency logged)
    speeches = [s for s in speeches if isinstance(s, dict)]

    # Add linkedMediaIndexes info - it indicates the cases where the
    # same proceeding has been linked with multiple media items.

    # For this case to be properly handled, we should split the
    # proceedings in the media (through speech recognition and text
    # alignment).
    proceeding2media = {}
    for speech in speeches:
        mid = speech['debug']['mediaIndex']
        for pi in speech['debug']['proceedingIndexes']:
            proceeding2media.setdefault(pi, set()).add(mid)
    # Now that we have built the index, put the info in each speech
    for speech in speeches:
        mid = speech['debug']['mediaIndex']
        linkedMediaIndexes = list(set(mid
                                      for pid in speech['debug']['proceedingIndexes']
                                      for mid in proceeding2media[pid]))
        speech['debug']['linkedMediaIndexes'] = linkedMediaIndexes

    # Backfill ParlaMint-derived person attributes (wid, wtype, type, firstname,
    # lastname, faction.wid/wtype) for people-mentions that the merger picked
    # up from media-side bare labels but didn't reconcile against a wid'd
    # proceeding person in the same matched-proceedings slice. Use a session-
    # wide name index so a chair/minister whose Q-ID lives in another
    # proceeding still gets linked here. Closes a ~2 % wid gap that NEL
    # would otherwise have to fill; for DE-17 (ParlaMint XML carries Q-IDs
    # natively) this brings merger output to ~99.9 % wid coverage.
    persons_by_label: dict = {}
    for p in proceedings.get('data', []):
        for person in p.get('people', []):
            label = person.get('label')
            if label and person.get('wid') and label not in persons_by_label:
                persons_by_label[label] = person
    if persons_by_label:
        _scalar_fields = ('wid', 'wtype', 'type', 'firstname', 'lastname')
        for speech in speeches:
            for person in speech.get('people', []):
                label = person.get('label')
                if not label or person.get('wid'):
                    continue
                src = persons_by_label.get(label)
                if src is None:
                    continue
                for f in _scalar_fields:
                    if src.get(f) and not person.get(f):
                        person[f] = src[f]
                # Faction: only enrich an existing dict. Never invent one.
                # Bundestag media titles encode "(role/faction)"; for ministerial
                # / government-capacity speeches the title is e.g. "(PSts/)" with
                # an *empty* faction slot — a deliberate signal that the speaker
                # is acting as Parliamentary State Secretary, not as a party MP.
                # Backfilling the standing CDU/CSU affiliation from proceedings
                # here would override that signal and mis-attribute the speech.
                src_fac = src.get('faction')
                cur_fac = person.get('faction')
                if (isinstance(src_fac, dict) and src_fac.get('wid')
                        and isinstance(cur_fac, dict)):
                    for f in ('wid', 'wtype'):
                        if src_fac.get(f) and not cur_fac.get(f):
                            cur_fac[f] = src_fac[f]
                    if src_fac.get('label') and not cur_fac.get('label'):
                        cur_fac['label'] = src_fac['label']

    # Let's fix dateStart/dateEnd: the official info is in proceedings
    # (sitzung-start/ende-uhrzeit), but the UTC offset is only defined
    # in media timestamps.
    utc_offset = media['meta']['dateStart'][-6:]
    # Check that we actually have a UTC offset
    if is_utc_offset(utc_offset) and not is_utc_offset(proceedings['meta']['dateStart'][-6:]):
        # Simply copy the UTC offset string at the end.
        dateStart = proceedings['meta']['dateStart'] + utc_offset
        dateEnd = proceedings['meta']['dateEnd'] + utc_offset

    # Update session info in all speeches
    for speech in speeches:
        speech['session']['dateStart'] = dateStart
        speech['session']['dateEnd'] = dateEnd

    # Enforce the speech-id model: DE has no joint id, so the top-level id
    # (which equals the text id) is dropped here after all internal use of it.
    for sp in speeches:
        normalize_speech_originid(sp)

    return { "meta": { **proceedings['meta'],
                       "schemaVersion": "1.0",
                       "dateStart": dateStart,
                       "dateEnd": dateEnd,
                       "processing": {
                           **proceedings['meta'].get('processing', {}),
                           **media['meta'].get('processing', {}),
                           "merge": datetime.now().isoformat('T', 'seconds'),
                       },
                      },
             "data": speeches
            }

def merge_files(proceedings_file: Path, media_file:Path, options) -> dict:
    try:
        with open(proceedings_file) as f:
            proceedings = json.load(f)
    except FileNotFoundError:
        proceedings = None
    try:
        with open(media_file) as f:
            media = json.load(f)
    except FileNotFoundError:
        media = None

    if media is None:
        logger.error("No media file for session")
        return dict()
    if proceedings is None:
        logger.debug("No proceedings - return media as temporary merged data")
        media['meta']['processing']['merge'] = datetime.now().isoformat('T', 'seconds')
        return media
    # Order media, according to dateStart
    return merge_data(proceedings, media, options)

def merge_session(session: str, config: "Config", options) -> Path:
    """Merge media/proceeding files for the session.

    Return the produced file Path
    """
    media_file = config.file(session, "media")
    proceedings_file = config.file(session, "proceedings")

    logger.debug(f"Merging {proceedings_file.name} and {media_file.name}")
    output = merge_files(proceedings_file, media_file, options)

    return config.save_data(output, session, "merged")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge proceedings and media files.")
    parser.add_argument("proceedings_file", type=str, nargs='?',
                        help="Proceedings file")
    parser.add_argument("media_file", type=str, nargs='?',
                        help="Media file")
    parser.add_argument("--debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    parser.add_argument("--output", metavar="DIRECTORY", type=str,
                        help="Output directory - if not specified, output with be to stdout")

    args = parser.parse_args()
    if args.media_file is None or args.proceedings_file is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel=logging.DEBUG
    logging.basicConfig(level=loglevel)

    p = Path(args.proceedings_file)
    m = Path(args.media_file)

    output = merge_files(p, m, args)
    if args.output:
        d = Path(args.output) / f"{output['meta']['session']}-merged.json"
        out = open(d, 'w')
    else:
        out = sys.stdout
    json.dump(output, out, indent=2, ensure_ascii=False)

