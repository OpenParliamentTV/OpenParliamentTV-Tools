#! /usr/bin/env python3
"""FI (Eduskunta) workflow.

Three sources joined per session:

- **verkkolähetys** broadcast page → per-speech video offsets + speaker +
  party + agenda topic + reply flag (the merge spine).
- **avoindata VaskiData** PTK XML → verbatim per-speech text (grafted on).
- **Wikidata SPARQL ⋈ MemberOfParliament** → ``entities.json`` for NEL.

Stage orchestration (merge → NEL → align → publish) is shared via
``optv.shared.workflow``. NER is intentionally absent from
``supported_stages`` — entity-fishing ships no Finnish KB. The pipeline runs in
Finnish throughout; the per-speech ``originalLanguage`` (fi/sv) is recorded as
data but does not switch models (Swedish-minority speeches degrade slightly).

Period semantics: ``--period`` is the vaalikausi (electoral term) start year
(``2023``). Session keys are ``{year}-{number:03d}`` (e.g. ``2026-058``); see
``common.py`` for the year+number ↔ ``session.number`` encoding.

Download discovers sessions from ``SaliDBIstunto`` for the term years, or takes
explicit ``--fi-session YYYY-NNN`` values (repeatable; avoids the API's
occasional ingest lag for the very latest session).
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
from .common import Config, parse_session_str, session_str, term_years
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_for_session
from .parsers.proceedings2json import parse_ptk
from .scraper.build_entity_dump import write_entity_dump
from .scraper.fetch_media import fetch_media
from .scraper.fetch_proceedings import fetch_proceedings
from .scraper.avoindata import filter_rows

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _session_in_scope(args, session):
    """Sessions are ``YYYY-NNN``; the period is the term start year, so accept
    any session whose year falls inside the term (not a string prefix match)."""
    try:
        year, _ = parse_session_str(session)
    except ValueError:
        return False
    if args.limit_to_period and year not in term_years(args.period):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


def _discover_sessions(period: int, **kw) -> list[tuple[int, int]]:
    """Plenary (TAYSISTUN) sessions for the term's years, from SaliDBIstunto."""
    found: list[tuple[int, int]] = []
    for year in term_years(period):
        rows = filter_rows("SaliDBIstunto", "IstuntoVPVuosi", str(year),
                           per_page=100, max_pages=20, **kw)
        for r in rows:
            if r.get("IstuntoTyyppi") != "TAYSISTUN":
                continue
            num = r.get("IstuntoNumero")
            try:
                found.append((year, int(num)))
            except (TypeError, ValueError):
                continue
    logger.info(f"Discovered {len(found)} plenary sessions across term {period}")
    return found


def _requested_sessions(args) -> list[tuple[int, int]]:
    if args.fi_session:
        return [parse_session_str(s) for s in args.fi_session]
    return _discover_sessions(args.period,
                              retry_count=args.retry_count,
                              retry_delay_max=args.retry_delay_max)


def _download(config, args):
    write_entity_dump(config)  # cheap; refreshes the Wikidata-derived dump
    for year, number in _requested_sessions(args):
        session = session_str(year, number)
        if args.limit_session and not re.match(args.limit_session, session):
            continue
        fetch_proceedings(config, year, number, force=args.force,
                          retry_count=args.retry_count, retry_delay_max=args.retry_delay_max)
        fetch_media(config, args.period, year, number, force=args.force,
                    retry_count=args.retry_count, retry_delay_max=args.retry_delay_max)


def _parse(config, args):
    sessions = [s for s in config.sessions() if _session_in_scope(args, s)]
    if not sessions:
        logger.info(f"No sessions in scope for period {args.period} — nothing to parse.")
        return
    for session in sessions:
        year, number = parse_session_str(session)

        ptk_path = config.raw_ptk(session)
        proc_out = config.file(session, "proceedings")
        if ptk_path.exists():
            stale = (args.force or not proc_out.exists()
                     or ptk_path.stat().st_mtime > proc_out.stat().st_mtime)
            if stale:
                logger.info(f"[{session}] parsing proceedings")
                doc = parse_ptk(ptk_path.read_bytes(), args.spacy_model, year, number)
                proc_out.parent.mkdir(parents=True, exist_ok=True)
                proc_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
                logger.info(f"[{session}] wrote {proc_out.name} ({len(doc['data'])} speeches)")
        else:
            logger.warning(f"[{session}] no PTK XML — proceedings will be empty (media-only)")

        event_path = config.raw_event(session)
        media_out = config.file(session, "media")
        if event_path.exists():
            stale = (args.force or not media_out.exists()
                     or event_path.stat().st_mtime > media_out.stat().st_mtime)
            if stale:
                logger.info(f"[{session}] parsing media")
                doc = parse_media_for_session(config, session)
                media_out.parent.mkdir(parents=True, exist_ok=True)
                media_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
                logger.info(f"[{session}] wrote {media_out.name} ({len(doc['data'])} media records)")


def _merge(config, session, args):
    doc = merge_session(config, session, period=args.period)
    merged_file = config.file(session, "merged", create=True)
    merged_file.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    return merged_file


def _align(config, session, args):
    merged_file = config.file(session, "merged")
    if not merged_file.exists():
        raise FileNotFoundError(f"[{session}] no merged file - cannot align")
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
    parser.add_argument("--session", dest="fi_session", action="append", default=[],
                        help="Session key to download (e.g. 2026-058). Repeatable. "
                             "When omitted, sessions are discovered from SaliDBIstunto.")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="FI Eduskunta workflow: download → parse → merge → NEL → align.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
