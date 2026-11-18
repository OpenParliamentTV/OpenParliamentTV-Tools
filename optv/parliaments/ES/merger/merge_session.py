#! /usr/bin/env python3

# Merge the ES media stream (per-speech video + speaker + timing, the
# authoritative anchor) with the proceedings stream (HTML-segmented verbatim
# text). Unlike the DE merger — which assumes two near-parallel streams — the
# Congreso media feed is the sparse anchor: every media item is a real video
# clip, while the proceedings carry extra chair interjections that have no
# clip of their own. So we align by speaker SURNAME in sequence (a global
# alignment that gaps non-matching turns) and attach each media item's matched
# text. Proceedings turns with no media (chair "Gracias…") are dropped from the
# per-speech output, which stays video-centric and schema-valid (every speech
# keeps its media).

from __future__ import annotations

import logging
logger = logging.getLogger('merge_session' if __name__ == '__main__' else __name__)

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys
import unicodedata
from optv.shared.sequence_align import align_equal_keys
from optv.shared.speech_id import normalize_speech_originid
from optv.shared.meta import build_meta, fill_original_language, now_iso

if __package__ is None or __package__ == "":
    _module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(_module_dir.parents[3]))
    __package__ = _module_dir.name

# Scoring: a surname match is strongly rewarded; a non-match diagonal is made
# prohibitively expensive so the alignment never pairs different speakers —
# it gaps them instead (cheap, since unmatched chair turns are expected).
MATCH_SCORE = 2
MISMATCH_SCORE = -100
GAP_SCORE = -1

# Chair-mode role tokens used as a transparent separator when absorbing
# same-surname proceeding turns into one media item. A proc turn flagged with
# any of these in `people[0].role` (or as the bare label/lastname when the
# Diario writes "La señora PRESIDENTA:") is the presiding officer interrupting
# mid-speech ("Señora X, vaya terminando.") and never owns a media clip of its
# own, so absorption can skip past it to find the speaker's continuation.
_CHAIR_ROLE_TOKENS = {
    "PRESIDENTE", "PRESIDENTA",
    "VICEPRESIDENTE", "VICEPRESIDENTA",
}


def remove_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))


def norm_surname(item: dict) -> str:
    """Normalised surname for matching (accent-folded, uppercased, hyphens as
    spaces: the Diario writes "Sánchez Pérez Castejón" for "Sánchez
    Pérez-Castejón" now and then)."""
    people = item.get('people') or []
    if not people:
        return ""
    p = people[0]
    name = p.get('lastname') or p.get('label') or ""
    return " ".join(remove_accents(name).upper().replace("-", " ").split())


def _is_chair_role(item: dict) -> bool:
    """True if the proc turn is the presiding officer in chair mode."""
    people = item.get('people') or []
    if not people:
        return False
    p = people[0]
    if remove_accents(p.get('role') or '').upper().strip() in _CHAIR_ROLE_TOKENS:
        return True
    # Some Diario chair markers carry no separate `role` field — the bare
    # role token IS the label/lastname (e.g. `La señora PRESIDENTA: …`).
    for key in ('lastname', 'label'):
        if remove_accents(p.get(key) or '').upper().strip() in _CHAIR_ROLE_TOKENS:
            return True
    return False


def align_by_surname(media: list, proceedings: list) -> dict:
    """Global alignment of media and proceedings on surname.

    Returns {media_index_in_list: [proceeding_item, ...]} — the proceedings
    turns whose text belongs to each media item. A media item may absorb
    same-surname turns immediately following its match (one speech the Diario
    split across a chair aside or a page break).
    """
    m_keys = [norm_surname(x) for x in media]
    p_keys = [norm_surname(x) for x in proceedings]
    n, k = len(media), len(proceedings)

    # Needleman-Wunsch over surnames (shared core). The high mismatch penalty
    # keeps non-equal diagonals out, so the returned matches are the same
    # media-index -> proceeding-index pairs the bespoke backtrack produced.
    matched: dict = {i: [] for i in range(n)}
    for mi, pj in align_equal_keys(m_keys, p_keys,
                                   match=MATCH_SCORE, mismatch=MISMATCH_SCORE,
                                   gap=GAP_SCORE):
        matched[mi].append(pj)

    # Absorb same-surname proceedings turns bracketing each match into the same
    # media item — one speech the Diario split across turns. Both directions
    # are walked because the NW backtrack tie-breaks toward the *latest*
    # diagonal, so when a chair interrupts a speaker ("Señora X, vaya
    # terminando.") and the speaker resumes ("Sí, termino. … Muchas
    # gracias."), the NW match lands on the closing fragment and the body is
    # left orphaned earlier in the sequence. The absorption walks transparently
    # past chair-mode interjections (`_is_chair_role`) so a body + chair-aside +
    # closing triplet by the same speaker reunites under one media clip.
    proc_owner = {}
    for mi, plist in matched.items():
        for pj in plist:
            proc_owner[pj] = mi

    def absorb(mi: int, direction: int) -> None:
        """Walk outward from the matched proc range, absorbing same-surname
        turns and stepping past chair-mode interjections.
        direction: +1 = forward from last match, -1 = backward from first."""
        if not matched[mi]:
            return
        cur = (matched[mi][-1] if direction > 0 else matched[mi][0]) + direction
        while 0 <= cur < k and cur not in proc_owner:
            if p_keys[cur] and p_keys[cur] == m_keys[mi]:
                if direction > 0:
                    matched[mi].append(cur)
                else:
                    matched[mi].insert(0, cur)
                proc_owner[cur] = mi
            elif _is_chair_role(proceedings[cur]):
                pass  # transparent separator — skip past without claiming
            else:
                break  # unrelated speaker (not chair) — stop
            cur += direction

    for mi in range(n):
        absorb(mi, +1)
        absorb(mi, -1)

    return {mi: [proceedings[pj] for pj in plist] for mi, plist in matched.items()}


def merge_text_contents(proc_items: list) -> tuple:
    """Join the matched turns' text into one entry per rendering.

    Returns (textContents, originalLanguage or None). A turn the Diario
    prints in a co-official language carries two entries: the text as spoken
    and its Spanish version. A speech gets the Spanish rendering, plus the
    as-spoken one when any of its turns has it, each concatenating every
    turn's text (a Spanish-only turn reads the same in both). The platform
    stores one text per (type, language), so the turns are never emitted as
    separate entries of the same language.
    """
    spoken, spanish = [], []
    language = None
    for p in proc_items:
        tcs = p.get('textContents') or []
        if not tcs:
            continue
        if len(tcs) > 1:
            language = language or tcs[0].get('language')
            spoken.append(tcs[0])
            spanish.append(tcs[-1])
        else:
            spoken.append(tcs[0])
            spanish.append(tcs[0])
    if not spanish:
        return [], None

    def joined(parts: list, lang: str) -> dict:
        tc = deepcopy(parts[0])
        tc['language'] = lang
        tc['textBody'] = [deepcopy(b) for part in parts for b in part.get('textBody', [])]
        return tc

    es = joined(spanish, spanish[-1].get('language'))
    if language is None:
        return [es], None
    return [joined(spoken, language), es], language


def merge_item(media_item: dict, proc_items: list, session_id: str) -> dict:
    """Build one Stage 2 speech: media (video/speaker/time/agenda) + matched text."""
    output = deepcopy(media_item)
    output.setdefault('debug', {})
    output['debug']['mediaIndex'] = media_item.get('speechIndex')

    if proc_items:
        first = proc_items[0]
        output['originID'] = first.get('originID') or first.get('originTextID')
        output['debug']['proceedingIndex'] = first.get('speechIndex')
        output['debug']['proceedingIndexes'] = [p.get('speechIndex') for p in proc_items]
        output['textContents'], original_language = merge_text_contents(proc_items)
        if original_language:
            output['originalLanguage'] = original_language
        pages = [p.get('debug', {}).get('page') for p in proc_items if p.get('debug', {}).get('page')]
        if pages:
            output['debug']['page'] = pages[0]
        output['debug']['confidence'] = 1.0
    else:
        # Video clip with no matched transcript turn (e.g. an oath, or a chair
        # turn the Diario logged only as "PRESIDENTA:"). Keep the clip; flag it.
        output['originID'] = f"{session_id}-m{media_item.get('speechIndex')}"
        output['textContents'] = []
        output['debug']['proceedingIndex'] = None
        output['debug']['proceedingIndexes'] = []
        output['debug']['confidence'] = 0.5
        output['debug']['confidenceReason'] = 'no-matched-text'

    output.setdefault('documents', [])
    return output


def merge_data(proceedings: dict, media: dict, options=None) -> dict:
    m = media.get('data', [])
    p = proceedings.get('data', []) if proceedings else []
    session_id = media.get('meta', {}).get('session', '')

    matched = align_by_surname(m, p) if p else {i: [] for i in range(len(m))}

    speeches = []
    for mi, media_item in enumerate(m):
        speeches.append(merge_item(media_item, matched.get(mi, []), session_id))

    for idx, s in enumerate(speeches):
        s['speechIndex'] = idx + 1
        s.setdefault('session', {})
        s['session']['dateStart'] = media.get('meta', {}).get('dateStart')
        s['session']['dateEnd'] = media.get('meta', {}).get('dateEnd')

    matched_count = sum(1 for s in speeches if s['textContents'])
    logger.info(f"{session_id}: {len(m)} media, {len(p)} proceedings turns, "
                f"{matched_count}/{len(speeches)} speeches with matched text")

    for _s in speeches:
        normalize_speech_originid(_s)
    fill_original_language(speeches, "ES")
    return {
        "meta": build_meta(
            "ES",
            session=session_id,
            electoral_period=(speeches[0].get('electoralPeriod') if speeches else None),
            date_start=media.get('meta', {}).get('dateStart'),
            date_end=media.get('meta', {}).get('dateEnd'),
            processing={
                **((proceedings.get('meta', {}).get('processing', {})) if proceedings else {}),
                **media.get('meta', {}).get('processing', {}),
                "merge": now_iso(),
            },
        ),
        "data": speeches,
    }


def merge_files(proceedings_file: Path, media_file: Path, options=None) -> dict:
    try:
        media = json.loads(Path(media_file).read_text())
    except FileNotFoundError:
        logger.error("No media file for session")
        return {}
    try:
        proceedings = json.loads(Path(proceedings_file).read_text())
    except FileNotFoundError:
        logger.debug("No proceedings - merging media without text")
        proceedings = None
    return merge_data(proceedings, media, options)


def merge_session(session: str, config: "Config", options=None) -> Path:
    """Merge media/proceeding files for the session. Return the produced Path."""
    media_file = config.file(session, "media")
    proceedings_file = config.file(session, "proceedings")
    logger.debug(f"Merging {proceedings_file.name} and {media_file.name}")
    output = merge_files(proceedings_file, media_file, options)
    return config.save_data(output, session, "merged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge ES proceedings and media files.")
    parser.add_argument("proceedings_file", type=str, nargs='?', help="Proceedings JSON file")
    parser.add_argument("media_file", type=str, nargs='?', help="Media JSON file")
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--output", metavar="DIRECTORY", type=str,
                        help="Output directory - if not specified, output to stdout")
    args = parser.parse_args()
    if args.media_file is None or args.proceedings_file is None:
        parser.print_help()
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    output = merge_files(Path(args.proceedings_file), Path(args.media_file), args)
    if args.output:
        d = Path(args.output) / f"{output['meta']['session']}-merged.json"
        d.parent.mkdir(parents=True, exist_ok=True)
        with open(d, 'w') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
    else:
        json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
