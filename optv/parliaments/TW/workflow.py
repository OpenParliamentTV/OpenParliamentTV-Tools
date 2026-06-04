#! /usr/bin/env python3
"""TW Legislative Yuan workflow.

Downloads per-speech IVODs (with whisperx + pyannote transcripts) from
``ly.govapi.tw/v2``, parses them, merges into Stage 2, and runs NEL + NER.

Period semantics: ``--period=11`` is the 11th term. Session keys are
``{屆:02d}{會期:02d}{會次:03d}`` strings (e.g. ``"1105011"`` =
``院會-11-5-11``). The default ``session_in_scope`` from the shared runner
(``session.startswith(str(args.period))``) works as long as ``--period`` is
the term number — which is the design.

Download requires ``--tw-meeting-code <院會-T-SP-MN>`` (repeatable) because
the LY API doesn't offer a "discover all current plenary meetings"
endpoint; the caller picks which plenary to ingest. To enumerate plenary
meetings within a session-period for backfill, see
:meth:`scraper.ly_api.LYApiClient.list_plenary_meeting_codes`.

Alignment is custom: TW skips aeneas entirely. The whisperx transcript
attached to each IVOD already has per-segment ``start`` / ``end`` seconds
which we treat as sentence-level timings, so the ``_align`` hook just
copies whisperx output into ``sentences[].timeStart/timeEnd`` and writes
``debug.align-duration`` on each speech.
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

from optv.shared.workflow import WorkflowHooks, run_main

from .common import Config
from .merger.merge_session import merge_session
from .parsers.media2json import parse_session_media
from .parsers.proceedings2json import parse_session_proceedings
from .parsers.transcript import whisperx_to_sentences, whisperx_max_time
from .scraper.fetch_media import download_media
from .scraper.fetch_proceedings import download_proceedings

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config: Config, args):
    download_media(config, args)
    download_proceedings(config, args)


def _parse(config: Config, args):
    for session in config.sessions():
        raw_ivods = config.file(session, "ivods")
        media_out = config.file(session, "media", create=True)
        media_stale = (
            args.force
            or not media_out.exists()
            or (raw_ivods.exists() and raw_ivods.stat().st_mtime > media_out.stat().st_mtime)
        )
        if media_stale and raw_ivods.exists():
            doc = parse_session_media(config, session)
            media_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {media_out.name} ({len(doc['data'])} media records)")

        raw_details = config.file(session, "details")
        proc_out = config.file(session, "proceedings", create=True)
        proc_stale = (
            args.force
            or not proc_out.exists()
            or (raw_details.exists() and raw_details.stat().st_mtime > proc_out.stat().st_mtime)
        )
        if proc_stale and raw_details.exists():
            doc = parse_session_proceedings(config, session)
            proc_out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
            logger.info(f"[{session}] wrote {proc_out.name} ({len(doc['data'])} speeches)")


def _merge(config: Config, session: str, args):
    return merge_session(session, config, args)


def _align(config: Config, session: str, args):
    """Custom align hook: read whisperx timings from the raw IVOD detail bundle
    and write per-sentence ``timeStart`` / ``timeEnd`` onto every speech.

    No external alignment runs (aeneas/espeak Mandarin quality is poor); the
    ly.govapi.tw API already serves WhisperX segment timings which are
    higher quality than what aeneas would produce.
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
        ivod_id = speech.get("originID") or ""
        wx = whisperx_by_id.get(str(ivod_id)) or []
        if not wx:
            continue
        sentences = whisperx_to_sentences(wx)
        if not sentences:
            continue
        # Replace sentences in the first speech textBody; if there's no
        # textContents yet (text-missing case), create one.
        tcs = speech.get("textContents") or []
        if not tcs:
            tcs = [{
                "type": "proceedings",
                "language": "zh-TW",
                "originTextID": str(ivod_id),
                "sourceURI": (speech.get("media") or {}).get("sourcePage") or "",
                "creator": "立法院 (Legislative Yuan)",
                "license": "https://data.gov.tw/license",
                "textBody": [],
            }]
            speech["textContents"] = tcs
        tb = tcs[0].setdefault("textBody", [])
        if not tb:
            tb.append({
                "type": "speech",
                "speaker": ((speech.get("people") or [{}])[0].get("label") or ""),
                "speakerstatus": None,
                "sentences": [],
            })
        # The whisperx segments are the authoritative source of timing AND
        # of text — overwrite both, since the proceedings parser used them
        # to build the textBody in the first place.
        tb[0]["sentences"] = sentences

        speech.setdefault("debug", {})
        speech["debug"]["align-duration"] = round(whisperx_max_time(wx), 3)
        speech["debug"]["align-source"] = "whisperx"
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
)


def _add_arguments(parser):
    """Parliament-specific flags beyond the shared set."""
    parser.add_argument("--session", dest="tw_meeting_code", action="append", default=[],
                        help="Plenary meeting code, e.g. 院會-11-5-11 (repeatable). "
                             "Required for --download-original.")
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
