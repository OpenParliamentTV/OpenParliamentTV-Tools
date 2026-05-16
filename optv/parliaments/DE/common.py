#! /usr/bin/env python3

import logging
logger = logging.getLogger(__name__)

from enum import Enum, auto
from hashlib import blake2b
import json
from pathlib import Path

class SessionStatus(Enum):
    media = auto()
    proceedings = auto()
    merged = auto()
    aligned = auto() # Time alignment info is present
    linked = auto() # Wikidata id for people/factions is present
    ner = auto() # Entities have been extracted from proceedings text
    session = auto()
    empty = auto()
    no_text = auto()

def data_signature(data: list) -> str:
    """Return a signature (as a string) for the given data.
    """
    h = blake2b(json.dumps(data).encode('utf-8'))
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
    """True if any speech carries time-alignment output."""
    return any(s.get('debug', {}).get('align-duration') for s in data)


def data_has_ner(data: list) -> bool:
    """True if any speech carries named-entity-recognition output."""
    return any(s.get('debug', {}).get('ner-duration') for s in data)


def is_demotion(new_data: list, published_data: list) -> bool:
    """True if publishing new_data over published_data would drop alignment
    or NER enrichment the published file already has.

    Keeps processed/ monotonic: a bare merged file (or any less-processed file
    produced from a stale cache) must never overwrite a richer published
    session.
    """
    if data_has_timing(published_data) and not data_has_timing(new_data):
        return True
    if data_has_ner(published_data) and not data_has_ner(new_data):
        return True
    return False


def _speech_key(speech: dict):
    """Stable per-speech identity for cross-stage matching.

    originID is dropped at top level in some outputs, so originTextID is the
    reliable key; speechIndex is the fallback.
    """
    return speech.get('originTextID') or speech.get('speechIndex')


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


class Config:
    def __init__(self, data_dir: Path,
                 cache_dir: Path = None):
        data_dir = Path(data_dir)
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
        else:
            cache_dir = data_dir / "cache"
        self._dir = {
            'data': data_dir,
            'cache': cache_dir,
            'media': data_dir / "original" / "media",
            'proceedings': data_dir / "original" / "proceedings",
            'merged': cache_dir / "merged",
            'aligned': cache_dir / "aligned",
            'ner': cache_dir / "ner",
            'processed': data_dir / "processed",
            'nel_data': data_dir / "metadata"
        }


    def dir(self, stage: str = 'processed', create: bool = False) -> Path:
        d = self._dir[stage]
        if create and not d.is_dir():
            d.mkdir(parents=True)
        return d


    def file(self, session: str, stage: str = 'processed', create = False) -> Path:
        suffix = stage
        d = self._dir[stage]
        if stage == 'processed':
            suffix = 'session'
        if create:
            # Make sure the containing directory exists
            if not d.is_dir():
                d.mkdir(parents=True)
        return d / f"{session}-{suffix}.json"


    def data(self, session: str, stage: str = 'processed') -> list:
        filename = self.file(session, stage)
        if filename.exists():
            with open(filename) as f:
                data = json.load(f)
        else:
            logger.warning(f"No data for {session}-{stage}")
            data = []
        return data


    def is_newer(self, session: str, stage: str, than: str) -> bool:
        """Check if the "stage" session file is newer than the "than" stage file.
        """
        stage_file = self.file(session, stage)
        than_file = self.file(session, than)
        return (not than_file.exists()
                or (stage_file.exists()
                    and stage_file.stat().st_mtime > than_file.stat().st_mtime))


    def save_data(self, data: list, session: str, stage: str) -> Path:
        """Serialize the given data into the appropriate file.

        Return the Path of the created file.
        """
        logger.debug(f"Saving {session} {stage} data")
        outfile = self.file(session, stage)
        # Make sure the containing directory exists
        if not outfile.parent.is_dir():
            outfile.parent.mkdir(parents=True)
        with open(outfile, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return outfile


    def sessions(self, prefix: str = ''):
        """Return the list of current existing sessions

        The list is built from the available media source files.
        """
        return list(sorted(f.name[4:9] for f in self.dir('media').glob(f'raw-{prefix}*-media.json')))


    def status(self, session: str) -> set:
        """Return the status for the given session.

        Return set of SessionStatus flags.
        """
        status = set()
        if self.file(session, 'media').exists():
            status.add(SessionStatus.media)
        if self.file(session, 'proceedings').exists():
            status.add(SessionStatus.proceedings)
        if self.file(session, 'merged').exists():
            status.add(SessionStatus.merged)
        sfile = self.file(session, 'processed')
        if sfile.exists():
            status.add(SessionStatus.session)
            with open(sfile, 'r') as f:
                info = json.load(f)
                data = info['data']
            if len(data) == 0:
                status.add(SessionStatus.empty)
            # Check for wid/wtype in people, in the first non-empty people list
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            # Check for proceedingIndex information (indication that proceedings were merged)
            for s in data:
                if s['debug'].get('proceedingIndex') is None:
                    status.add(SessionStatus.no_text)
                    return status
            # Trying to find at least 1 timeStart attribute
            # for s in data:
            #     for tc in s['textContents']:
            #         for b in tc['textBody']:
            #             for sentence in b['sentences']:
            #                 if sentence.get('timeStart') is not None:
            #                     status.add('aligned')
            #                     break
            # Just test on s['debug']['align-duration']
                if s.get('debug', {}).get('align-duration'):
                    status.add(SessionStatus.aligned)
                if s.get('debug', {}).get('ner-duration'):
                    status.add(SessionStatus.ner)

        return status

if __name__ == '__main__':
    import sys
    config = Config(Path(sys.argv[1]))
    import IPython
    IPython.embed()

