#! /usr/bin/env python3
"""PT (Assembleia da República) workflow.

Two sources, joined by a speaker-sequence alignment (DE two-source pattern):

- **av.parlamento.pt JSON API** (per-meeting) → the media spine: one
  intervention per speech with speaker / party / interventionType / per-speech
  video clip + offsets. No transcript text.
- **debates.parlamento.pt ``?sft=true``** → verbatim DAR text (the
  ``textContents``), matched onto the av interventions by surname / chair role.
- **Wikidata SPARQL** (P39 Q19953703 members; P102 parties) → ``entities.json``
  for NEL (av carries no Wikidata/BID).

Stage orchestration (merge → NEL → align → NER → publish) is shared via
``optv.shared.workflow``. NER runs in Portuguese (entity-fishing ``pt`` + spaCy
``pt_core_news_md``); the Portuguese KB (``db-pt``) must be loaded into the
entity-fishing instance — see the README.

Period semantics: ``--period`` is the legislatura (``17``). Session keys are
``{leg}-{sl}-{meeting:03d}`` (e.g. ``17-1-059``); see ``common.py`` for the
``session.number`` encoding. ``--pt-session`` takes explicit keys; otherwise the
legislatura's reuniões are enumerated from av.parlamento.pt.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.align import align_audio
from optv.shared.workflow import WorkflowHooks, run_main

from .align_prep import prepare_per_speech_audio
from .common import Config, parse_session
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_directory
from .parsers.proceedings2json import parse_proceedings_directory
from .scraper.build_entity_dump import write_entity_dump
from .scraper.fetch_media import download_media
from .scraper.fetch_proceedings import download_proceedings

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _session_in_scope(args, session):
    """Session keys are ``{leg}-{sl}-{meeting}``. Scope = key validity (correct
    legislatura) plus the ``--limit-session`` filter."""
    try:
        leg, _sl, _meeting = parse_session(session)
    except ValueError:
        return False
    if getattr(args, "period", None) and leg != int(args.period):
        return False
    if args.limit_session:
        try:
            return bool(re.match(args.limit_session, session))
        except re.error:
            return args.limit_session == session
    return True


def _download(config, args):
    entities = config.dir("nel_data") / "entities.json"
    if args.force or not entities.exists():
        try:
            write_entity_dump(config, args.period)
        except Exception as e:  # noqa: BLE001
            logger.error(f"entity dump build failed ({e}); NEL may be degraded")
    download_media(config, args)        # av JSON first (supplies the date)
    download_proceedings(config, args)  # debates ?sft=true text


def _parse(config, args):
    parse_media_directory(config, args)
    parse_proceedings_directory(config, args)


def _merge(config, session, args):
    return merge_session(session, config, args)


def _align(config, session, args):
    merged_file = config.file(session, "merged")
    if not merged_file.exists():
        raise FileNotFoundError(f"[{session}] no merged file — cannot align")
    doc = json.loads(merged_file.read_text())

    logger.info(f"[{session}] preparing per-speech audio slices")
    prepare_per_speech_audio(doc["data"], args.cache_dir, force=args.force)

    logger.info(f"[{session}] running aeneas alignment ({args.aeneas_language})")
    align_audio(doc["data"], language=args.aeneas_language, cachedir=args.cache_dir,
                force=args.force, timeout=args.align_timeout,
                max_audio_seconds=args.align_max_audio_seconds)
    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    doc["meta"].setdefault("processing", {})["align"] = now
    doc["meta"]["lastProcessing"] = "align"
    doc["meta"]["lastUpdate"] = now

    aligned_file = config.file(session, "aligned", create=True)
    aligned_file.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {aligned_file.name}")
    return aligned_file


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
    session_in_scope=_session_in_scope,
)


def _add_arguments(parser):
    """Parliament-specific flags beyond the shared set."""
    parser.add_argument("--session", action="append", default=[], dest="pt_session",
                        help="Session key to process (e.g. 17-1-059). Repeatable. "
                             "When omitted, the legislatura's reuniões are enumerated.")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="PT Assembleia da República workflow: download → parse → "
                    "merge → NEL → align → NER.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
