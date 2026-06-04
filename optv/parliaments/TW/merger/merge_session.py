#! /usr/bin/env python3
"""Merge parsed TW media and proceedings into one Stage 2 session file.

Join key: ``IVOD_ID``. Each per-speech clip has its own IVOD_ID in both
streams, so the merge is a 1:1 left join from media → proceedings (media is
the spine; if a per-speech clip exists but has no transcript yet, the merged
speech carries empty text + a ``debug.merge.text-missing`` flag).

Output shape matches ``optv/shared/schema/stage2-full.schema.json``:
``{"meta": {...}, "data": [speech, ...]}``. Each merged speech inherits
the media block from the media parser, the agenda/speaker/text from the
proceedings parser, and gets ``speechIndex`` from chronological position
in the merged stream.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from copy import deepcopy
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                          # TW/
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))     # repo root
    __package__ = "optv.parliaments.TW.merger"

from optv.parliaments.TW.common import Config, decode_session
from optv.shared.agenda_types import CORE_REGULAR
from optv.shared.speech_id import normalize_speech_originid
from optv.shared.publish import save_if_changed

logger = logging.getLogger(__name__)

PARLIAMENT_CODE = "TW"


def _index_proceedings(proceedings_data: list[dict]) -> dict[str, dict]:
    """``{originID -> speech}``. originID is the stringified IVOD_ID."""
    index: dict[str, dict] = {}
    for s in proceedings_data:
        key = str(s.get("originID") or "")
        if not key:
            continue
        if key in index:
            logger.warning(f"duplicate IVOD_ID={key} in proceedings; keeping first")
        else:
            index[key] = s
    return index


def merge_one(media_record: dict, proceeding: dict | None,
              *, term: int, session_number: int, speech_index: int) -> dict:
    """Build one merged speech. ``proceeding`` is None when no transcript exists."""
    ivod_id = media_record.get("IVOD_ID")
    speech: dict = {
        "parliament": PARLIAMENT_CODE,
        "electoralPeriod": {"number": term},
        "session": {"number": session_number},
        "speechIndex": speech_index,
        "media": deepcopy(media_record["media"]),
        "originID": str(ivod_id) if ivod_id is not None else "",
        "isReply": False,
    }
    if media_record.get("dateStart"):
        speech["dateStart"] = media_record["dateStart"]
    if media_record.get("dateEnd"):
        speech["dateEnd"] = media_record["dateEnd"]

    speaker_label = (media_record.get("speakerLabel") or "").strip()
    meeting_name = media_record.get("meetingName") or ""

    debug: dict = {
        "ivod_id": ivod_id,
        "mediaIndex": speech_index,
    }

    if proceeding is None:
        # Media without proceedings text. Synthesize minimal valid fields.
        speech["agendaItem"] = {
            "officialTitle": meeting_name,
            "title": meeting_name,
            "type": CORE_REGULAR,
            "nativeType": "TW-plenary",
        }
        speech["people"] = [{
            "type": "memberOfParliament",
            "label": speaker_label or "（不詳）",
            "context": "main-speaker",
        }] if (speaker_label or meeting_name) else []
        speech["textContents"] = []
        debug["merge"] = {"text-missing": True}
    else:
        speech["agendaItem"] = deepcopy(proceeding["agendaItem"])
        speech["people"] = deepcopy(proceeding.get("people") or [])
        speech["textContents"] = deepcopy(proceeding.get("textContents") or [])
        debug["proceedingIndex"] = proceeding.get("speechIndex", speech_index)
        # Carry parser breadcrumbs forward (whisperx_segments, has_gazette, ...).
        for k, v in (proceeding.get("debug") or {}).items():
            debug.setdefault(k, v)

    speech["debug"] = debug
    return speech


def merge_session(session: str, config: Config, args=None) -> Path:
    """Merge one session's parsed media+proceedings into a Stage 2 file."""
    media_path = config.file(session, "media")
    proc_path = config.file(session, "proceedings")
    if not media_path.exists():
        sys.exit(f"[{session}] parsed media missing: {media_path}")
    if not proc_path.exists():
        sys.exit(f"[{session}] parsed proceedings missing: {proc_path}")

    media_doc = json.loads(media_path.read_text())
    proc_doc = json.loads(proc_path.read_text())

    media_data = media_doc.get("data") or []
    proc_data = proc_doc.get("data") or []
    if not media_data:
        sys.exit(f"[{session}] no media records to merge")

    term, sp, mn = decode_session(session)
    session_number = sp * 1000 + mn

    proc_index = _index_proceedings(proc_data)
    text_missing = 0
    merged: list[dict] = []
    for idx, m in enumerate(media_data, start=1):
        proc = proc_index.get(str(m.get("IVOD_ID") or ""))
        if proc is None:
            text_missing += 1
        merged.append(merge_one(m, proc,
                                term=term,
                                session_number=session_number,
                                speech_index=idx))

    media_keys = {str(m.get("IVOD_ID") or "") for m in media_data}
    proc_only = [k for k in proc_index if k not in media_keys]
    if proc_only:
        logger.warning(
            f"[{session}] {len(proc_only)} proceedings entry/entries without "
            f"matching media — dropped (IVOD_IDs={proc_only[:5]}"
            f"{'…' if len(proc_only) > 5 else ''})."
        )

    logger.info(
        f"[{session}] merged {len(merged)} speeches "
        f"({len(merged) - text_missing} with text, {text_missing} text-missing)"
    )

    media_meta = media_doc.get("meta") or {}
    proc_meta = proc_doc.get("meta") or {}
    date_start = media_meta.get("dateStart") or proc_meta.get("dateStart")
    date_end = media_meta.get("dateEnd") or proc_meta.get("dateEnd")

    for _s in merged:
        normalize_speech_originid(_s)
    doc = {
        "meta": {
            "session": session,
            "schemaVersion": "1.0",
            "meetingCode": media_meta.get("meetingCode") or proc_meta.get("meetingCode"),
            "dateStart": date_start,
            "dateEnd": date_end,
            "lastUpdate": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            "lastProcessing": "merge",
            "processing": {
                **(proc_meta.get("processing") or {}),
                **(media_meta.get("processing") or {}),
                "merge": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": merged,
    }
    out = config.file(session, "merged", create=True)
    if save_if_changed(doc, out):
        logger.info(f"[{session}] wrote {out.name}")
    else:
        logger.info(f"[{session}] no content change; left {out.name} untouched")
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = Config(args.data_dir)
    merge_session(args.session, config, args)


if __name__ == "__main__":
    main()
