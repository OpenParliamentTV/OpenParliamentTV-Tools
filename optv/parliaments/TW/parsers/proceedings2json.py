#! /usr/bin/env python3
"""Parse IVOD detail bundles into per-speech proceedings records.

Input:  ``original/proceedings/{session}-details.json``  — fetch_proceedings
                                                            output (one record
                                                            per per-speech IVOD,
                                                            with transcripts).
Output: ``original/proceedings/{session}-proceedings.json`` — per-speech text
                                                              records (Stage 2
                                                              shape, no media).

For each IVOD detail we emit a speech record with:

* ``people[0].label`` = ``委員名稱``
* ``textContents[].textBody[].sentences`` = whisperx segments, with
  ``timeStart`` / ``timeEnd`` already populated (we treat each whisperx
  segment as one sentence — see :mod:`.transcript`).
* ``originID`` = ``IVOD_ID``
* ``agendaItem.officialTitle`` = ``會議名稱`` (the plenary's full title is the
  best agenda anchor we have — TW doesn't expose per-speech agenda items).

If an IVOD has no transcript at all (neither whisperx nor gazette), we still
emit the record with an empty ``textContents`` so the merger doesn't lose
the speech; the merger flags it with ``debug.merge.text-missing``.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                          # TW/
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))     # repo root
    __package__ = "optv.parliaments.TW.parsers"

from optv.parliaments.TW.common import Config, decode_session
from optv.shared.agenda_types import CORE_REGULAR
from .transcript import (
    gazette_paragraphs,
    whisperx_max_time,
    whisperx_to_sentences,
)
from optv.parliaments import get_rights as _get_rights
from optv.parliaments import get_language as _get_language

logger = logging.getLogger(__name__)

PARLIAMENT_CODE = "TW"
LANGUAGE_CODE = _get_language("TW")
SPEECH_CREATOR = _get_rights("TW", stream="proceedings")["creator"]
# Open Government Data License (Taiwan), CC-BY-4.0 compatible.
SPEECH_LICENSE = _get_rights("TW", stream="proceedings")["license"]


def _session_number(session_period: int, meeting_number: int) -> int:
    return session_period * 1000 + meeting_number


def _make_textbody_from_whisperx(segments: list[dict], speaker: str) -> dict | None:
    sentences = whisperx_to_sentences(segments)
    if not sentences:
        return None
    return {
        "type": "speech",
        "speaker": speaker,
        "speakerstatus": None,
        "sentences": sentences,
    }


def _make_textbody_from_gazette(paragraphs: list[str], speaker: str) -> dict | None:
    if not paragraphs:
        return None
    # Each paragraph becomes one untimed sentence; downstream we have no
    # better signal in the gazette case. (Whisperx is the priority path.)
    return {
        "type": "speech",
        "speaker": speaker,
        "speakerstatus": None,
        "sentences": [{"text": p} for p in paragraphs],
    }


def speech_record(ivod_detail: dict, *, term: int, session_period: int,
                  meeting_number: int, speech_index: int) -> dict:
    """One IVOD detail → intermediate proceedings record."""
    ivod_id = ivod_detail.get("IVOD_ID")
    speaker = (ivod_detail.get("委員名稱") or "").strip()
    meeting_name = (
        ivod_detail.get("會議名稱")
        or (ivod_detail.get("會議資料") or {}).get("標題")
        or ""
    )

    transcript = ivod_detail.get("transcript") or {}
    whisperx = transcript.get("whisperx") or []
    gazette = ivod_detail.get("gazette")

    text_body: list[dict] = []
    tb = _make_textbody_from_whisperx(whisperx, speaker)
    if tb is not None:
        text_body.append(tb)
    else:
        tb_gaz = _make_textbody_from_gazette(gazette_paragraphs(gazette), speaker)
        if tb_gaz is not None:
            text_body.append(tb_gaz)

    text_contents: list[dict] = []
    if text_body:
        text_contents.append({
            "type": "proceedings",
            "language": LANGUAGE_CODE,
            "originTextID": str(ivod_id) if ivod_id is not None else "",
            "sourceURI": ivod_detail.get("IVOD_URL") or "",
            "creator": SPEECH_CREATOR,
            "license": SPEECH_LICENSE,
            "textBody": text_body,
        })

    person: dict[str, Any] = {
        "type": "memberOfParliament",
        "label": speaker or "（不詳）",  # schema requires minLength 1
        "context": "main-speaker",
    }

    record: dict[str, Any] = {
        "parliament": PARLIAMENT_CODE,
        "electoralPeriod": {"number": term},
        "session": {"number": _session_number(session_period, meeting_number)},
        "agendaItem": {
            "officialTitle": meeting_name,
            "title": meeting_name,
            "type": CORE_REGULAR,
            "nativeType": "TW-plenary",
        },
        "speechIndex": speech_index,
        "originID": str(ivod_id) if ivod_id is not None else "",
        "isReply": False,
        "people": [person],
        "textContents": text_contents,
        "documents": [],
        "debug": {
            "ivodId": ivod_id,
            "whisperxSegments": len(whisperx),
            "hasGazette": bool(gazette),
        },
    }

    date_start = ivod_detail.get("開始時間")
    date_end = ivod_detail.get("結束時間")
    if date_start:
        record["dateStart"] = date_start
    if date_end:
        record["dateEnd"] = date_end
    # Carry the whisperx audio length on the record so the merger can
    # emit `debug.align-duration` deterministically.
    record["debug"]["whisperxLastEnd"] = whisperx_max_time(whisperx)
    return record


def parse_details(doc: dict, *, term: int, session_period: int,
                  meeting_number: int) -> dict:
    """Convert a raw details bundle into the parsed proceedings doc.

    Speeches are 1-indexed by `開始時間` ordering (deterministic).
    """
    meta = dict(doc.get("meta") or {})
    session_key = meta.get("session") or ""
    ivods = list(doc.get("ivods") or [])
    ivods.sort(key=lambda i: (i.get("開始時間") or "", i.get("IVOD_ID") or 0))

    speeches: list[dict] = []
    for idx, detail in enumerate(ivods, start=1):
        speeches.append(speech_record(detail,
                                      term=term,
                                      session_period=session_period,
                                      meeting_number=meeting_number,
                                      speech_index=idx))

    out_meta = {
        "session": session_key,
        "meetingCode": meta.get("meetingCode"),
        "processing": {
            "parse_proceedings": datetime.datetime.utcnow().isoformat(timespec="seconds"),
        },
    }
    if speeches:
        out_meta["dateStart"] = speeches[0].get("dateStart")
        out_meta["dateEnd"] = max(
            (s.get("dateEnd") for s in speeches if s.get("dateEnd")),
            default=None,
        )
    return {"meta": out_meta, "data": speeches}


def parse_session_proceedings(config: Config, session: str) -> dict:
    """Read the raw details bundle and return the parsed proceedings doc."""
    raw_path = config.file(session, "details")
    if not raw_path.exists():
        raise FileNotFoundError(f"No raw IVOD details at {raw_path}")
    doc = json.loads(raw_path.read_text())
    term, sp, mn = decode_session(session)
    return parse_details(doc, term=term, session_period=sp, meeting_number=mn)


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
    doc = parse_session_proceedings(config, args.session)
    out = config.file(args.session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} speeches)")


if __name__ == "__main__":
    main()
