#! /usr/bin/env python3
"""NL Tweede Kamer workflow.

Fetches the report of each plenary sitting from the House's open data API and the sitting's debates from Debat Direct, parses and merges them per session, then optionally links entities and aligns sentences. Stage orchestration is shared via ``optv.shared.workflow``.

``--period`` is the year a House was elected (``2025``). Session keys are ``{session year}-{sitting number}``, e.g. ``2026-005``; see ``common.py``.

Which sittings a download covers: those named with ``--session`` (repeatable), otherwise every plenary sitting of the period, confined by ``--limit-session``.

A sitting whose report was replaced by a newer version (the corrected final report follows the interim one weeks later) is queued by the scraper in ``cache/nl-report-upgrades.txt``; the shared runner re-runs the queued sittings with rebuild semantics after the regular stages.
"""

import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.NL.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.audio_prep import make_align_hook
from optv.shared.workflow import WorkflowHooks, adapt_merge, run_main

from .align_prep import prepare_per_speech_audio
from .common import Config, in_scope
from .merger.merge_session import merge_session
from .parsers.media2json import parse_media
from .parsers.proceedings2json import parse_proceedings
from .scraper.fetch import UPGRADES_FILE, fetch_originals

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    fetch_originals(config, args)


def _parse(config, args):
    parse_media(config, args)
    parse_proceedings(config, args)


_merge = adapt_merge(merge_session)

_align = make_align_hook(prepare_per_speech_audio)


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
    session_in_scope=in_scope,
    rebuild_queue=UPGRADES_FILE,
)


def _add_arguments(parser):
    parser.add_argument("--session", dest="nl_session", action="append", default=[],
                        help="Session key to download, e.g. 2026-005 (repeatable). "
                             "Without it the download covers every sitting of the period.")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="NL Tweede Kamer workflow: download, parse, merge, link entities, align.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
