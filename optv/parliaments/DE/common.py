#! /usr/bin/env python3

import logging
logger = logging.getLogger(__name__)

from enum import Enum, auto
import json
from pathlib import Path

class SessionStatus(Enum):
    media = auto()
    proceedings = auto()
    merged = auto()
    aligned = auto()
    ner = auto()
    session = auto()
    empty = auto()
    no_text = auto()


class Config:
    def __init__(self, data_dir: Path,
                 cache_dir: Path | None = None):
        cache_dir = cache_dir or (data_dir / "cache")
        self._dir = {
            'data': data_dir,
            'cache': cache_dir,
            'media': data_dir / "original" / "media",
            'proceedings': data_dir / "original" / "proceedings",
            'merged': cache_dir / "merged",
            'aligned': cache_dir / "aligned",
            'ner': cache_dir / "ner",
            'processed': data_dir / "processed"
        }

    def dir(self, stage: str = 'processed', create: bool = False) -> Path:
        d = self._dir[stage]
        if create and not d.is_dir():
            d.mkdir(parents=True)
        return d

    def file(self, session: str, stage: str = 'processed') -> Path:
        d = self._dir[stage]
        return d / f"{session}-{stage}.json"

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

    def sessions(self, prefix: str = ''):
        """Return the list of current existing sessions

        The list is built from the available media source files.
        """
        return [ f.name[4:9] for f in self.dir('media').glob(f'raw-{prefix}*-media.json') ]

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
                data = json.load(f)
            if len(data) == 0:
                status.add(SessionStatus.empty)
            for s in data:
                if s['agendaItem'].get('proceedingIndex') is None:
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
                status.add('aligned')
            if s.get('debug', {}).get('ner-duration'):
                status.add('ner')

        return status
