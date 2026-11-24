#! /usr/bin/env python3
"""IT Camera dei deputati workflow.

Fetches the WebTV page of each sitting of the Chamber and its stenographic record, parses and merges them per session, then optionally links entities, aligns sentences and extracts entities. Stage orchestration is shared via ``optv.shared.workflow``.

``--period`` is the legislature (``19``). Session keys are ``{legislature}{sitting:04d}``, e.g. ``190731``; see ``common.py``.

Which sittings a download covers: those named with ``--session`` (repeatable), otherwise every sitting of the legislature the WebTV archive lists, confined by ``--limit-session``.
"""

import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.IT.workflow`.
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
from .scraper.fetch import fetch_originals

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
)


def _add_arguments(parser):
    parser.add_argument("--session", dest="it_session", action="append", default=[],
                        help="Session key to download, e.g. 190731 (repeatable). "
                             "Without it the download covers every sitting of the legislature.")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="IT Camera dei deputati workflow: download, parse, merge, link entities, align, extract entities.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
