#! /usr/bin/env python3
"""AT Nationalrat workflow.

Reverse-engineers parlament.gv.at's SvelteKit Mediathek (the per-speech video +
protocol spine), joins the stenographic-protocol text onto it by exact
``std_id``, then optionally links entities, aligns sentences and extracts named
entities. Stage orchestration is shared via ``optv.shared.workflow``.

Session keys are ``{period}{sitting:03d}`` (e.g. ``27144``). ``--download-original``
fetches either an explicit list of sittings (``--sitting 144 --sitting 200``) or,
with none given, walks the whole period's Mediathek pages to discover them.

Alignment audio is each speech's server-trimmed HLS window
(``media.videoFileURI`` with ``?startseconds=…&stopseconds=…``): the per-speech
MP3/MP4 clip assets the source also exposes are unreliable (sometimes absent,
sometimes the whole session), so ``align_prep`` transcodes the trimmed HLS per
speech and the shared ``make_align_hook`` drives aeneas over it.
"""

import json
import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.AT.workflow`.
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
from .parsers.media2json import parse_session as parse_media_session
from .parsers.proceedings2json import parse_session as parse_proceedings_session
from .scraper.fetch_session import discover_sittings, fetch_session

logger = logging.getLogger(__name__ if __name__ != '__main__' else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args):
    sittings = list(args.sitting) if args.sitting else None
    if sittings is None:
        logger.info(f"No --sitting given; discovering sittings for period {args.period}")
        sittings = discover_sittings(args.period, retry_count=args.retry_count,
                                     retry_delay_max=args.retry_delay_max)
        logger.info(f"Discovered {len(sittings)} sitting(s): {sittings[:10]}"
                    f"{'…' if len(sittings) > 10 else ''}")
    for sitting in sittings:
        fetch_session(config, args.period, sitting, force=args.force,
                      retry_count=args.retry_count, retry_delay_max=args.retry_delay_max)


def _parse(config, args):
    """Parse downloaded sittings into intermediate media/proceedings JSON.

    Re-runs only when the raw Mediathek payload is newer than the parsed output
    (the protocol HTMLs are fetched alongside the payload, so its mtime gates
    both streams)."""
    for session in config.sessions():
        if not session.startswith(str(args.period)):
            continue
        raw = config.dir("media") / f"{session}-mediathek.json"
        if not raw.exists():
            continue
        raw_mtime = raw.stat().st_mtime

        media_out = config.file(session, "media")
        if args.force or not media_out.exists() or raw_mtime > media_out.stat().st_mtime:
            doc = parse_media_session(config, session, args.period)
            config.file(session, "media", create=True).write_text(
                json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] parsed media ({len(doc['data'])} records)")

        proc_out = config.file(session, "proceedings")
        if args.force or not proc_out.exists() or raw_mtime > proc_out.stat().st_mtime:
            doc = parse_proceedings_session(config, session, args.period)
            config.file(session, "proceedings", create=True).write_text(
                json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] parsed proceedings ({len(doc['data'])} records)")


def _merge(config, session, args):
    from .common import save_if_changed
    doc = merge_session(config, session, args.period)
    merged_file = config.file(session, "merged", create=True)
    save_if_changed(doc, merged_file)
    return merged_file


# Per-speech audio = each speech's server-trimmed HLS window (see align_prep);
# the shared hook stages it, runs aeneas and writes the aligned cache file.
_align = make_align_hook(prepare_per_speech_audio)


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
)


def _add_arguments(parser):
    parser.add_argument("--sitting", type=int, action="append", default=[],
                        help="Sitting number to download (repeatable). Omit to discover "
                             "the whole period by walking the Mediathek pages.")


def main():
    run_main(PARLIAMENT_ID, HOOKS,
             description="AT Nationalrat workflow: download → parse → merge → NEL → align → NER.",
             add_arguments=_add_arguments,
             config_cls=Config)


if __name__ == "__main__":
    main()
