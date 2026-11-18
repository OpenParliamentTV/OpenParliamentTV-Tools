#! /usr/bin/env python3
"""TW Legislative Yuan workflow.

Downloads per-speech IVODs (with WhisperX + pyannote transcripts) from
``ly.govapi.tw/v2``, the per-legislator speech kinds from open data dataset
221 and the official per-clip text from the IVOD site, parses them, merges
into Stage 2, and runs NEL + the WhisperX timing step.

Period semantics: ``--period=11`` is the 11th term. Session keys are
``{屆:02d}{會期:02d}{會次:03d}`` strings (e.g. ``"1105011"`` =
``院會-11-5-11``). The default ``session_in_scope`` from the shared runner
(``session.startswith(str(args.period))``) works as long as ``--period`` is
the term number — which is the design.

Download scope: ``--session <院會-T-SP-MN>`` (repeatable) fetches those
plenary meetings; without it, plenary meetings with IVODs on any day from
``--since`` (default: the last 14 days) to today are discovered by date.

Text: each speech carries the official text (``textContents[].type
"proceedings"``, untimed) once the IVOD site publishes it, plus the WhisperX
machine transcript (``"generated"``, timed). Official text that arrives for an
already-published session is queued in ``cache/tw-official-text-upgrades.txt``
for a rebuild pass, which the shared runner runs after the regular stages
(``rebuild_queue``).

Alignment is custom: TW skips aeneas entirely. The WhisperX transcript
attached to each IVOD already has per-segment ``start`` / ``end`` seconds
which we treat as sentence-level timings, so the ``_align`` hook copies them
into the ``generated`` entry's ``sentences[].timeStart/timeEnd`` and writes
``debug.alignDuration`` on each speech. The official text stays untimed.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import sys
from pathlib import Path

# Allow ./workflow.py and `python -m optv.parliaments.TW.workflow`.
if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = module_dir.name

from optv.shared.workflow import WorkflowHooks, adapt_merge, run_main

from .common import OFFICIAL_TEXT_UPGRADES_FILE, Config
from .merger.merge_session import merge_session
from .parsers.media2json import parse_session_media
from .parsers.proceedings2json import generated_text_content, parse_session_proceedings
from .parsers.transcript import whisperx_to_sentences, whisperx_max_time
from .scraper.fetch_media import download_media
from .scraper.fetch_proceedings import download_proceedings
from .scraper.fetch_records import download_records
from .scraper.fetch_speech_kinds import download_speech_kinds

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config: Config, args):
    download_media(config, args)
    download_speech_kinds(config, args)
    download_proceedings(config, args)
    download_records(config, args)


def _stale(output: Path, inputs: list[Path], force: bool) -> bool:
    """Output missing, forced, or older than any existing input."""
    if force or not output.exists():
        return True
    mtime = output.stat().st_mtime
    return any(p.exists() and p.stat().st_mtime > mtime for p in inputs)


def _parse(config: Config, args):
    for session in config.sessions():
        raw_ivods = config.file(session, "ivods")
        media_out = config.file(session, "media", create=True)
        media_inputs = [raw_ivods, config.file(session, "speechkinds")]
        if raw_ivods.exists() and _stale(media_out, media_inputs, args.force):
            doc = parse_session_media(config, session)
            media_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {media_out.name} ({len(doc['data'])} media records)")

        raw_details = config.file(session, "details")
        proc_out = config.file(session, "proceedings", create=True)
        proc_inputs = [raw_details, config.file(session, "records")]
        if raw_details.exists() and _stale(proc_out, proc_inputs, args.force):
            doc = parse_session_proceedings(config, session)
            proc_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {proc_out.name} ({len(doc['data'])} speeches)")


_merge = adapt_merge(merge_session)


def _align(config: Config, session: str, args):
    """Custom align hook: read WhisperX timings from the raw IVOD detail bundle
    and write per-sentence ``timeStart`` / ``timeEnd`` onto the ``generated``
    (WhisperX) textContents entry of every speech.

    No external alignment runs (aeneas/espeak Mandarin quality is poor); the
    ly.govapi.tw API already serves WhisperX segment timings which are
    higher quality than what aeneas would produce. The official
    ``proceedings`` entry is left untouched (untimed).
    """
    merged_file = config.file(session, "merged")
    if not merged_file.exists():
        raise FileNotFoundError(f"[{session}] no merged file — cannot align")

    details_file = config.file(session, "details")
    if not details_file.exists():
        raise FileNotFoundError(
            f"[{session}] no raw details bundle at {details_file}; "
            "re-run --download-original."
        )

    details_doc = json.loads(details_file.read_text())
    whisperx_by_id: dict[str, list[dict]] = {}
    for detail in details_doc.get("ivods") or []:
        ivod_id = detail.get("IVOD_ID")
        if ivod_id is None:
            continue
        wx = ((detail.get("transcript") or {}).get("whisperx")) or []
        whisperx_by_id[str(ivod_id)] = wx

    merged_doc = json.loads(merged_file.read_text())
    aligned_count = 0
    for speech in merged_doc.get("data") or []:
        ivod_id = (speech.get("media") or {}).get("originMediaID") or speech.get("originID") or ""
        wx = whisperx_by_id.get(str(ivod_id)) or []
        if not wx:
            continue
        sentences = whisperx_to_sentences(wx)
        if not sentences:
            continue
        tcs = speech.setdefault("textContents", [])
        generated = next((tc for tc in tcs if tc.get("type") == "generated"), None)
        speaker = ((speech.get("people") or [{}])[0].get("label") or "")
        if generated is None:
            tcs.append(generated_text_content(ivod_id, sentences, speaker))
        else:
            # The WhisperX segments are the source of both timing and text of
            # this entry — overwrite the sentences of its single textBody.
            tb = generated.setdefault("textBody", [])
            if not tb:
                tb.append({"type": "speech", "speaker": speaker,
                           "speakerstatus": None, "sentences": []})
            tb[0]["sentences"] = sentences

        speech.setdefault("debug", {})
        speech["debug"]["alignDuration"] = round(whisperx_max_time(wx), 3)
        speech["debug"]["alignSource"] = "whisperx"
        media = speech.setdefault("media", {})
        media["aligned"] = True
        aligned_count += 1

    merged_doc["meta"].setdefault("processing", {})
    merged_doc["meta"]["processing"]["align"] = _dt.datetime.utcnow().isoformat(timespec="seconds")
    merged_doc["meta"]["lastProcessing"] = "align"
    merged_doc["meta"]["lastUpdate"] = _dt.datetime.utcnow().isoformat(timespec="seconds")

    aligned_file = config.file(session, "aligned", create=True)
    aligned_file.write_text(json.dumps(merged_doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {aligned_file.name} (aligned {aligned_count}/{len(merged_doc['data'])})")
    return aligned_file


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
    rebuild_queue=OFFICIAL_TEXT_UPGRADES_FILE,
)


def _add_arguments(parser):
    """Parliament-specific flags beyond the shared set."""
    parser.add_argument("--session", dest="tw_meeting_code", action="append", default=[],
                        help="Plenary meeting code to download, e.g. 院會-11-5-11 "
                             "(repeatable). Without it, plenary meetings are "
                             "discovered by date (see --since).")
    parser.add_argument("--since", type=_dt.date.fromisoformat, default=None,
                        help="Discover plenary meetings with IVODs from this date "
                             "(YYYY-MM-DD) to today. Default: the last 14 days.")
    parser.add_argument("--limit-ivods", type=int, default=None,
                        help="Fetch at most N IVODs per session (testing only)")


def main():
    run_main(
        PARLIAMENT_ID, HOOKS,
        description="TW Legislative Yuan workflow: download → parse → merge → "
                    "align (whisperx) → NEL → NER.",
        add_arguments=_add_arguments,
        config_cls=Config,
    )


if __name__ == "__main__":
    main()
