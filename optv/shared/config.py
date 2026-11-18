#! /usr/bin/env python3
"""Shared per-parliament file-layout + session-status base class.

``BaseConfig`` holds the layout every parliament shares
(``original/{media,proceedings}``, ``cache/{merged,aligned,ner}``,
``processed/``, ``metadata/``) and the ``dir`` / ``file`` / ``data`` /
``is_newer`` / ``save_data`` / ``sessions`` / ``status`` helpers. Each
parliament's ``optv/parliaments/<CODE>/common.py`` subclasses it as ``Config``
and sets class attributes for what differs:

- ``sessions()`` — ``SESSION_SOURCE`` / ``MEDIA_GLOB_PREFIX`` /
  ``MEDIA_GLOB_SUFFIX`` name the raw files a session is discovered from
  (``raw-…-media.json``, ``…-media.json``, ``…-anforanden.json`` under
  proceedings, …).
- ``status()`` — text-bearing parliaments use the full probe; video-only
  parliaments set ``HAS_TEXT = False`` to short-circuit to ``no_text`` (no
  align/ner stages exist for them). Media-spine parliaments, where a few clips
  per session routinely have no matched text, set ``TEXT_PROBE = "any"`` so
  such sessions still count as text-bearing. ``TEXT_SIGNAL`` says what marks a
  speech as carrying text.
- ``EXTRA_DIRS`` — further stage directories beyond the shared layout.

Each ``common.py`` also imports ``SessionStatus``: the Conductor reads it,
together with ``Config``, from ``optv.parliaments.<CODE>.common`` in-process.
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
    # --- status knob: when a text-bearing session is ``no_text`` ---
    # "every": no_text as soon as one speech lacks merged text (proceedings
    # spine: an unmatched speech means the merge is incomplete).
    # "any": no_text only when no speech has merged text (media spine: a few
    # clips per session without a Diario turn are expected; align and NER
    # skip those speeches and process the rest).
    TEXT_PROBE: str = "every"
    # --- status knob: what marks a speech as carrying merged text ---
    # "proceedingIndex": the merger stamps ``debug.proceedingIndex`` (DE family,
    # the PDF-tier spine join). "textContents": a non-empty ``textBody`` in any
    # ``textContents`` entry (mergers that graft text without that stamp).
    TEXT_SIGNAL: str = "proceedingIndex"
    # --- extra stage directories: name -> (base, relative path) ---
    # ``base`` is "data" or "cache", e.g.
    # ``{"meetings": ("data", "original/meetings")}``. A name may alias one of
    # the shared directories, which gives ``file(session, name)`` its own
    # ``{session}-{name}.json`` there.
    EXTRA_DIRS: dict = {}

    def has_merged_text(self, speech: dict) -> bool:
        """Whether ``speech`` carries merged text, by ``TEXT_SIGNAL``."""
        if self.TEXT_SIGNAL == "textContents":
            return any((tc.get('textBody') or [])
                       for tc in (speech.get('textContents') or []))
        return (speech.get('debug') or {}).get('proceedingIndex') is not None

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
        for name, (base, relative) in self.EXTRA_DIRS.items():
            self._dir[name] = (cache_dir if base == "cache" else data_dir) / relative

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
        transcript, so they short-circuit to ``no_text``. ``TEXT_PROBE``
        decides whether one speech without merged text makes the whole
        session ``no_text`` ("every") or only a session where no speech has
        text ("any"); ``TEXT_SIGNAL`` decides what counts as merged text.
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
            # Merged text per speech (TEXT_SIGNAL), probed per TEXT_PROBE.
            if self.TEXT_PROBE == "any":
                with_text = [s for s in data if self.has_merged_text(s)]
                if not with_text:
                    status.add(SessionStatus.no_text)
                    return status
                data = with_text
            for s in data:
                if not self.has_merged_text(s):
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
