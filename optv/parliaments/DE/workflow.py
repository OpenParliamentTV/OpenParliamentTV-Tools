#! /usr/bin/env python3
"""DE Bundestag workflow.

Fetches Bundestag media (per-speech video + metadata) and proceedings (TEI
XML from period 18 onwards, ParlaMint-DE for periods 16-17), parses each
into the intermediate JSON shape, merges them per session, then optionally
aligns, links entities, and extracts entities. Stage orchestration is
shared via ``optv.shared.workflow``.
"""

import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.DE.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.align import align_audiofile
from optv.shared.workflow import WorkflowHooks, run_main

from .common import Config
from .merger.merge_session import merge_session
from .parsers.parlamint2json import parse_parlamint_directory
from .parsers.proceedings2json import parse_proceedings_directory
from .scraper.fetch_parlamint import (REGISTRY_FILES, download_parlamint_period,
                                      download_parlamint_registries)
from .scraper.fetch_proceedings import download_plenary_protocols
from .scraper.update_media import update_media_directory_period, update_media_from_raw

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name

# Periods served by the ParlaMint-DE_beta corpus (Bundestag native TEI is
# only available from period 18 onwards).
PARLAMINT_PERIODS = {16, 17}


def _download(config, args):
    logger.info(f"Downloading media and proceeding data for period {args.period}")
    update_media_directory_period(args.period, config.dir('media'),
                                  force=args.force, save_raw_data=True,
                                  retry_count=args.retry_count)
    if args.period in PARLAMINT_PERIODS:
        download_parlamint_period(args.period, config.dir('proceedings'), force=args.force)
    else:
        download_plenary_protocols(config.dir('proceedings'), fullscan=False, period=args.period)


def _ensure_parlamint_registries(proceedings_dir: Path) -> None:
    """Fetch listPerson/listOrg if absent.

    Neither the registries nor the parsed `-proceedings.json` are versioned in
    the data repo, so a fresh checkout has no way to parse a ParlaMint period
    unless we fetch them here -- `--download-original` is not required for a
    merge-only run. No-op once they are on disk.
    """
    missing = [name for name in REGISTRY_FILES
               if not (proceedings_dir / name).exists()]
    if not missing:
        return
    logger.warning(f"ParlaMint registries missing ({', '.join(missing)}) - fetching")
    try:
        download_parlamint_registries(proceedings_dir)
    except Exception as e:
        # parse_parlamint_directory degrades to skipping sessions it cannot
        # re-parse, so a transient network failure need not kill the run.
        logger.error(f"Could not fetch ParlaMint registries: {e}")


def _parse(config, args):
    if args.period in PARLAMINT_PERIODS:
        _ensure_parlamint_registries(config.dir('proceedings'))
        parse_parlamint_directory(config.dir('proceedings'), args)
    else:
        parse_proceedings_directory(config.dir('proceedings'), args)
    update_media_from_raw(config.dir('media'))


def _merge(config, session, args):
    return merge_session(session, config, args)


def _align(config, session, args):
    merged_file = config.file(session, 'merged')
    aligned_file = config.file(session, 'aligned', create=True)
    align_audiofile(merged_file, aligned_file, args.lang, args.cache_dir,
                    timeout=args.align_timeout,
                    max_audio_seconds=args.align_max_audio_seconds)
    return aligned_file


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
)


def main():
    # `--lang` and `--retry-count` are now shared flags (build_common_argparser),
    # defaulting from the manifest; the Conductor cron passes them explicitly.
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="DE Bundestag workflow: fetch, parse, merge, align, NEL, NER.",
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
