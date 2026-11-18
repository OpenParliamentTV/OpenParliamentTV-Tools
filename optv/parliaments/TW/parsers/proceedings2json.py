#! /usr/bin/env python3
"""Parse IVOD detail bundles into per-speech proceedings records.

Input:  ``original/proceedings/{session}-details.json``  — fetch_proceedings
                                                            output (one record
                                                            per per-speech IVOD,
                                                            with transcripts).
        ``original/proceedings/{session}-records.json``  — fetch_records
                                                            output (official
                                                            per-clip text, only
                                                            for clips that have
                                                            it yet).
Output: ``original/proceedings/{session}-proceedings.json`` — per-speech text
                                                              records (Stage 2
                                                              shape, no media).

For each IVOD detail we emit a speech record with:

* ``people[0].label`` = ``委員名稱``
* ``textContents``, in this order:

  1. ``type: "proceedings"``: the official text (the gazette record) once the
     IVOD site serves it (``ivod.ly.gov.tw/Demand/Speech/{IVOD_ID}``, parsed
     by :mod:`.speech_record` into one ``textBody`` item per speaker turn), or
     the ``gazette`` field of the detail where ly.govapi.tw carries one.
     Untimed. Rights from the manifest ``proceedings`` stream.
  2. ``type: "generated"``: the WhisperX machine transcript made by
     ly.govapi.tw, one sentence per segment with ``timeStart`` / ``timeEnd``
     (see :mod:`.transcript`). Rights from the manifest ``transcript`` stream.

* ``originID`` = ``IVOD_ID``

``agendaItem`` is not set here: the merger builds it from the media record
(dataset-221 speech kind, or the sitting title).

If an IVOD has no text at all, we still emit the record with an empty
``textContents`` so the merger doesn't lose the speech; the merger flags it
with ``debug.merge.text-missing``.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
from pathlib import Path
from typing import Any

from optv.parliaments.TW.common import SPEECH_RECORD_URL, Config, decode_session
from .speech_record import parse_speech_record
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
# Official text: the Legislative Yuan's open-data declaration.
PROCEEDINGS_RIGHTS = _get_rights("TW", stream="proceedings")
# WhisperX machine transcript by ly.govapi.tw.
TRANSCRIPT_RIGHTS = _get_rights("TW", stream="transcript")

IVOD_API_URL = "https://ly.govapi.tw/v2/ivods/{ivod_id}"


def _session_number(session_period: int, meeting_number: int) -> int:
    return session_period * 1000 + meeting_number


def generated_text_content(ivod_id, sentences: list[dict], speaker: str) -> dict:
    """The ``generated`` (WhisperX) textContents entry for one clip.

    One ``textBody`` item for the whole clip: the transcript is not split by
    speaker, so it also carries the answers of the officials questioned.
    """
    return {
        "type": "generated",
        "language": LANGUAGE_CODE,
        "originTextID": str(ivod_id) if ivod_id is not None else "",
        "sourceURI": IVOD_API_URL.format(ivod_id=ivod_id),
        "creator": TRANSCRIPT_RIGHTS["creator"],
        "license": TRANSCRIPT_RIGHTS["license"],
        "textBody": [{
            "type": "speech",
            "speaker": speaker,
            "speakerstatus": None,
            "sentences": sentences,
        }],
    }


def _official_text_content(ivod_id, text_body: list[dict], source_uri: str) -> dict:
    return {
        "type": "proceedings",
        "language": LANGUAGE_CODE,
        "originTextID": str(ivod_id) if ivod_id is not None else "",
        "sourceURI": source_uri,
        "creator": PROCEEDINGS_RIGHTS["creator"],
        "license": PROCEEDINGS_RIGHTS["license"],
        "textBody": text_body,
    }


def _gazette_text_body(paragraphs: list[str], speaker: str) -> list[dict]:
    if not paragraphs:
        return []
    # Each paragraph becomes one untimed sentence.
    return [{
        "type": "speech",
        "speaker": speaker,
        "speakerstatus": None,
        "sentences": [{"text": p} for p in paragraphs],
    }]


def speech_record(ivod_detail: dict, *, term: int, session_period: int,
                  meeting_number: int, speech_index: int,
                  official_html: str | None = None) -> dict:
    """One IVOD detail → intermediate proceedings record.

    ``official_html`` is the clip's official text fragment from the IVOD
    site, if it has been published.
    """
    ivod_id = ivod_detail.get("IVOD_ID")
    speaker = (ivod_detail.get("委員名稱") or "").strip()

    transcript = ivod_detail.get("transcript") or {}
    whisperx = transcript.get("whisperx") or []
    gazette = ivod_detail.get("gazette")

    text_contents: list[dict] = []
    official_body = parse_speech_record(official_html or "", speaker)
    if official_body:
        text_contents.append(_official_text_content(
            ivod_id, official_body, SPEECH_RECORD_URL.format(ivod_id=ivod_id)))
    else:
        gazette_body = _gazette_text_body(gazette_paragraphs(gazette), speaker)
        if gazette_body:
            text_contents.append(_official_text_content(
                ivod_id, gazette_body, IVOD_API_URL.format(ivod_id=ivod_id)))
    sentences = whisperx_to_sentences(whisperx)
    if sentences:
        text_contents.append(generated_text_content(ivod_id, sentences, speaker))

    person: dict[str, Any] = {
        "type": "memberOfParliament",
        "label": speaker or "（不詳）",  # schema requires minLength 1
        "context": "main-speaker",
    }

    record: dict[str, Any] = {
        "parliament": PARLIAMENT_CODE,
        "electoralPeriod": {"number": term},
        "session": {"number": _session_number(session_period, meeting_number)},
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
            "officialText": bool(official_body),
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
                  meeting_number: int, records: dict[str, str] | None = None) -> dict:
    """Convert a raw details bundle into the parsed proceedings doc.

    ``records`` maps IVOD_ID → official text fragment (see
    :mod:`..scraper.fetch_records`). Speeches are 1-indexed by `開始時間`
    ordering (deterministic).
    """
    records = records or {}
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
                                      speech_index=idx,
                                      official_html=records.get(str(detail.get("IVOD_ID")))))

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
    records_path = config.file(session, "records")
    records = None
    if records_path.exists():
        records = json.loads(records_path.read_text()).get("records") or {}
    term, sp, mn = decode_session(session)
    return parse_details(doc, term=term, session_period=sp, meeting_number=mn,
                         records=records)


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
