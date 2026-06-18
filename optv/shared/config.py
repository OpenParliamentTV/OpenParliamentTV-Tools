#! /usr/bin/env python3
"""Shared per-parliament file-layout + session-status base class.

Every parliament's ``optv/parliaments/<CODE>/common.py`` defined an essentially
identical ``Config`` class: the same ``original/{media,proceedings}`` +
``cache/{merged,aligned,ner}`` + ``processed/`` + ``metadata/`` layout, and the
same ``dir`` / ``file`` / ``data`` / ``is_newer`` / ``save_data`` helpers. Those
move here once as ``BaseConfig``; each parliament subclasses it and overrides
only what genuinely differs:

- ``sessions()`` — parameterized via the ``MEDIA_GLOB_PREFIX`` /
  ``MEDIA_GLOB_SUFFIX`` / ``SESSION_SOURCE`` class attributes (covers the
  ``raw-…-media.json`` and the bare ``…-media.json`` families); parliaments
  whose session discovery is genuinely different (EU's ``cre_`` prefix, the
  ``…-anforanden.json`` proceedings-keyed discoverers) keep overriding it.
- ``status()`` — text-bearing parliaments use the full probe (the DE behaviour,
  verbatim); video-only parliaments set ``HAS_TEXT = False`` to short-circuit to
  ``no_text`` (no align/ner stages exist for them).

Subclasses keep re-exporting ``SessionStatus`` and the ``optv.shared.publish``
helpers from their ``common.py`` for back-compat with callers (including the
Conductor's in-process ``optv.parliaments.<CODE>.common`` import).
"""

import json
import logging
from pathlib import Path

from optv.shared.session_status import SessionStatus

logger = logging.getLogger(__name__)


class BaseConfig:
    # --- session-discovery knobs (override per parliament where needed) ---
    SESSION_SOURCE: str = "media"          # which stage dir to enumerate
    MEDIA_GLOB_PREFIX: str = ""            # e.g. "raw-" for DE / DE-RP / ES
    MEDIA_GLOB_SUFFIX: str = "-media.json"
    # --- status knob: False ⇒ video-only (no transcript, no align/ner) ---
    HAS_TEXT: bool = True

    def __init__(self, data_dir: Path, cache_dir: Path = None):
        data_dir = Path(data_dir)
        cache_dir = Path(cache_dir) if cache_dir is not None else data_dir / "cache"
        self._dir = {
            'data': data_dir,
            'cache': cache_dir,
            'media': data_dir / "original" / "media",
            'proceedings': data_dir / "original" / "proceedings",
            'merged': cache_dir / "merged",
            'aligned': cache_dir / "aligned",
            'ner': cache_dir / "ner",
            'processed': data_dir / "processed",
            'nel_data': data_dir / "metadata",
        }

    def dir(self, stage: str = 'processed', create: bool = False) -> Path:
        d = self._dir[stage]
        if create and not d.is_dir():
            d.mkdir(parents=True)
        return d

    def file(self, session: str, stage: str = 'processed', create=False) -> Path:
        suffix = 'session' if stage == 'processed' else stage
        d = self._dir[stage]
        if create and not d.is_dir():
            d.mkdir(parents=True)
        return d / f"{session}-{suffix}.json"

    def data(self, session: str, stage: str = 'processed') -> list:
        filename = self.file(session, stage)
        if filename.exists():
            with open(filename) as f:
                return json.load(f)
        logger.warning(f"No data for {session}-{stage}")
        return []

    def is_newer(self, session: str, stage: str, than: str) -> bool:
        """True if the ``stage`` session file is newer than the ``than`` file."""
        stage_file = self.file(session, stage)
        than_file = self.file(session, than)
        return (not than_file.exists()
                or (stage_file.exists()
                    and stage_file.stat().st_mtime > than_file.stat().st_mtime))

    def save_data(self, data: list, session: str, stage: str) -> Path:
        """Serialize ``data`` into the appropriate file; return its Path."""
        logger.debug(f"Saving {session} {stage} data")
        outfile = self.file(session, stage)
        if not outfile.parent.is_dir():
            outfile.parent.mkdir(parents=True)
        with open(outfile, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return outfile

    def sessions(self, prefix: str = ''):
        """List existing sessions, derived from the source-stage files.

        Default: enumerate ``SESSION_SOURCE`` for ``{PREFIX}{prefix}*{SUFFIX}``
        and strip the prefix/suffix to recover the session key. Covers the
        ``raw-21001-media.json`` (DE family) and ``23018-media.json``
        (video-only family) shapes.
        """
        pre = self.MEDIA_GLOB_PREFIX
        suf = self.MEDIA_GLOB_SUFFIX
        names = self.dir(self.SESSION_SOURCE).glob(f'{pre}{prefix}*{suf}')
        return list(sorted(f.name[len(pre):-len(suf)] for f in names))

    def status(self, session: str) -> set:
        """Return the set of ``SessionStatus`` flags for ``session``.

        Text-bearing parliaments run the full probe (proceedings-merge →
        ``no_text``, ``align-duration`` → ``aligned``, ``ner-duration`` →
        ``ner``). Video-only parliaments (``HAS_TEXT = False``) ship without a
        transcript, so they short-circuit to ``no_text``.
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
            # Check for wid/wtype in people, in the first non-empty people list.
            # Runs for video-only parliaments too (NEL applies), so the
            # ``linked`` flag the Conductor uses to gate the nel stage is set.
            for s in data:
                if s.get('people') and s['people'][0].get('wid'):
                    status.add(SessionStatus.linked)
                    break
            if not self.HAS_TEXT:
                # Video-only: PDF-only/absent proceedings, no align/ner stages.
                status.add(SessionStatus.no_text)
                return status
            # Check for proceedingIndex information (indication that proceedings were merged)
            for s in data:
                if s.get('debug', {}).get('proceedingIndex') is None:
                    status.add(SessionStatus.no_text)
                    return status
                # Accept the legacy kebab keys too, mirroring data_has_timing /
                # data_has_ner in publish.py. Legacy-pipeline sessions carry
                # align-duration / ner-duration; checking only the camelCase
                # spelling here left them without the aligned/ner flag, so the
                # align and ner stages re-ran them from scratch on every job even
                # though the merge demotion guard (which does accept both keys)
                # already recognized them as enriched.
                debug = s.get('debug') or {}
                if debug.get('alignDuration') or debug.get('align-duration'):
                    status.add(SessionStatus.aligned)
                if debug.get('nerDuration') or debug.get('ner-duration'):
                    status.add(SessionStatus.ner)
        return status
