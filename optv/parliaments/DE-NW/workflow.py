#! /usr/bin/env python3
"""DE-NW Landtag Nordrhein-Westfalen workflow.

Enumerates session video pages (the paginated Mediathek archive +
operator-supplied ``kid``s), fetches each session's static ``TEST-REDNER``
per-speech spine and the precise per-speech offsets, then parses, merges, and
NEL-links the per-Sitzung speeches through the shared pipeline. Stage
orchestration lives in ``optv.shared.workflow``.

The verbatim Plenarprotokoll (``MMP18-{N}.pdf``) is now parsed via
``optv.shared.pdf2tei`` and joined onto the media spine in the merger
(``join_text_to_spine``), so ``align`` and ``ner`` are wired: ``_align`` slices
each text-bearing speech's audio from the session HLS stream and runs aeneas.
This text+align path is **experimental and unvalidated** — no Whisper-QC /
text-fidelity audit has cleared it yet, and substantial refinement is still
needed before platform integration.
"""

import logging
import os
import re
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-NW.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.audio_prep import make_align_hook
from optv.shared.workflow import WorkflowHooks, run_main

from .align_prep import prepare_per_speech_audio
from .common import Config
from .merger.merge_session import merge_session
from .parsers.proceedings2json import parse_proceedings_directory
from .scraper.fetch_proceedings import fetch_proceedings
from .parsers.media2json import parse_media_directory
from .scraper.fetch_archive import fetch_archive
from .scraper.fetch_media import fetch_media_for_archive

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    seeds = list(getattr(args, "session_url", []) or [])
    if getattr(args, "kid", None):
        seeds.extend(args.kid)
    archive = fetch_archive(
        period=args.period,
        media_dir=config.dir('media', create=True),
        metadata_dir=config.dir('nel_data', create=True),
        seed_urls=seeds,
        max_pages=getattr(args, "max_pages", None),
        force=args.force,
        retry_count=args.retry_count,
    )
    fetch_media_for_archive(
        archive=archive,
        media_dir=config.dir('media'),
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
    fetch_proceedings(config, args)


def _parse(config, args):
    parse_media_directory(config.dir('media'))
    parse_proceedings_directory(config, args)


def _merge(config, session, args):
    return merge_session(session, config, args)


def _session_in_scope(args, session: str) -> bool:
    """DE-NW session keys are 5-digit ``{period:02d}{sitzung:03d}`` (e.g. 18117).

    Mirror DE-HH/DE-BW/DE-BY: the shared default
    ``session.startswith(str(args.period))`` is correct for two-digit periods
    (18…), but the explicit override keeps a future single-digit term safe via
    zero-padding.
    """
    if args.limit_to_period and not session.startswith(f"{int(args.period):02d}"):
        return False
    if args.limit_session and not re.match(args.limit_session, session):
        return False
    return True


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=make_align_hook(prepare_per_speech_audio),
    session_in_scope=_session_in_scope,
)


def _add_arguments(parser):
    """Parliament-specific flags beyond the shared set."""
    parser.add_argument("--session", dest="session_url", action="append", default=[],
                        help="Session video-page URL or kid to include in the "
                             "archive (repeatable). Manual override for the "
                             "discovery enumeration.")
    parser.add_argument("--kid", action="append", default=[],
                        help="Session UUID (kid) to fetch directly, skipping "
                             "archive pagination (repeatable).")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Upper archive-page bound for the discovery "
                             "enumeration (default: read from archive page 1).")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-NW Landtag Nordrhein-Westfalen workflow.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
