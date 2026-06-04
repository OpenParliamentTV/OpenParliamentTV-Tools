#! /usr/bin/env python3
"""DE-SH Landtag Schleswig-Holstein workflow.

Scrapes the m7k mediathek's AJAX endpoints (``result.php``, the four
``*-html-selector.php`` selectors, and ``iframe.php?b=…``) plus the
Plenarprotokoll listing page for the date → Sitzung-number map, then
parses, merges, and NEL-links the per-Sitzung speeches through the shared
pipeline. Stage orchestration lives in ``optv.shared.workflow``.

``align`` and ``ner`` are intentionally not registered: the verbatim
transcripts live only in PDF Plenarprotokolle, and we don't yet have a
PDF parser. The published Stage 2 carries video + speaker + agenda
metadata with an empty ``textContents`` list; once a proceedings spine
exists those stages can be re-enabled.
"""

import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE-SH.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.workflow import WorkflowHooks, run_main

from .common import Config
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media_directory
from .scraper.fetch_archive import fetch_archive
from .scraper.fetch_media import fetch_media_for_archive

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    metadata_dir = config.dir('nel_data', create=True)
    archive = fetch_archive(
        period=args.period,
        media_dir=config.dir('media', create=True),
        metadata_dir=metadata_dir,
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


def _parse(config, args):
    parse_media_directory(config.dir('media'))


def _merge(config, session, args):
    return merge_session(session, config, args)


def _session_in_scope(args, session: str) -> bool:
    """DE-SH session keys are 5-digit ``{period:02d}{sitzung:03d}`` (e.g. 20119).

    The shared default uses ``session.startswith(str(args.period))`` which is
    correct for two-digit periods (20…), but we keep the override explicit so
    that any future single-digit term (rolling out a new Wahlperiode would
    bump the period to 21) still matches via zero-padding.
    """
    import re
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
    align_session_to_file=None,   # transcript text unavailable — see manifest
    session_in_scope=_session_in_scope,
)


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE-SH Landtag Schleswig-Holstein workflow.",
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
