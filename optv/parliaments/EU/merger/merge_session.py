#! /usr/bin/env python3
"""Merge EU CRE proceedings and glcloud media into Stage 2 JSON per plenary day.

Input::

    cache/proceedings_intermediate/{YYYYMMDD}-proceedings.json
    cache/media_intermediate/{YYYYMMDD}-media.json

Output::

    cache/merged/{YYYYMMDD}-merged.json   (Stage 2 shape, validates against
                                            optv/shared/schema/stage2-full.schema.json)

Per the EU.md plan, the join key is the per-speech VOD timestamp from CRE:
each speech's ``dateStart`` (UTC) is matched against each sitting's
``sittingStart``..``sittingEnd`` window. The matching sitting's HLS URLs (OR
audio + master video) are attached to the speech's media block, with the
``startOffset`` computed as ``dateStart - sittingStart`` (seconds).

Speeches without VOD timestamps (typically written submissions that were never
delivered on the floor) are skipped with a warning.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from optv.shared.agenda_types import classify_eu_native, annotate_agenda_item
from optv.shared.speech_id import normalize_speech_originid

logger = logging.getLogger(__name__)

PARLIAMENT = "EU"
ELECTORAL_PERIOD = 10


def _epoch(iso_str: str | None) -> int | None:
    if not iso_str:
        return None
    try:
        return int(datetime.fromisoformat(iso_str).timestamp())
    except (ValueError, TypeError):
        return None


def _find_sitting(speech_start_epoch: int, sittings: list[dict]) -> dict | None:
    for s in sittings:
        if s["sittingStart"] <= speech_start_epoch <= s["sittingEnd"]:
            return s
    return None


def _speech_record(
    speech: dict,
    sitting: dict,
    session: str,
    session_date_iso_start: str,
    session_date_iso_end: str,
) -> dict | None:
    start_epoch = _epoch(speech.get("dateStart"))
    end_epoch = _epoch(speech.get("dateEnd"))
    if start_epoch is None or end_epoch is None:
        return None
    start_offset = max(0, start_epoch - sitting["sittingStart"])
    duration = max(0, end_epoch - start_epoch)

    speaker = speech.get("speaker") or {}
    speaker_name = speaker.get("name") or ""
    faction_abbr = speaker.get("factionAbbr")
    faction_label = speaker.get("factionLabel")
    role = speaker.get("role")

    person_record = {
        "type": "memberOfParliament" if faction_abbr else "guest",
        "label": speaker_name,
        "context": "main-speaker",
    }
    if role:
        person_record["role"] = role
    if speaker.get("epId"):
        person_record["additionalInformation"] = {
            "epId": speaker["epId"],
            "photoURL": speaker.get("photoURL"),
        }
    if faction_abbr:
        person_record["faction"] = {
            "label": faction_label or faction_abbr,
            "abbr": faction_abbr,
        }

    paragraphs = speech.get("textParagraphs") or []
    sentences = [{"text": p, "entities": []} for p in paragraphs if p.strip()]
    text_contents = [{
        "type": "proceedings",
        "language": "en",
        "originTextID": speech.get("speechId") or "",
        "sourceURI": speech.get("debug", {}).get("vodURL") or "",
        "textBody": [{
            "type": "speech",
            "sentences": sentences,
        }],
    }]

    agenda = speech.get("agendaItem") or {}
    agenda_title = agenda.get("officialTitle") or "Untitled agenda item"
    native_type, core_type = classify_eu_native(agenda_title)
    agenda_item = {
        "officialTitle": agenda_title,
        "title": agenda_title,
    }
    annotate_agenda_item(agenda_item, native_type, core_type)
    if agenda.get("number") is not None:
        agenda_item["number"] = agenda["number"]

    media_audio = (sitting.get("hlsAudioUrls") or {}).get("or") or ""
    media_video = sitting.get("hlsMasterUrl") or ""
    if not media_video:
        return None

    media_block = {
        "videoFileURI": media_video,
        "sourcePage": speech.get("debug", {}).get("vodURL") or sitting.get("hlsMasterUrl") or "",
        "audioFileURI": media_audio,
        "duration": float(duration),
        "aligned": False,
        "creator": "European Parliament",
        "originMediaID": sitting.get("eventRef") or "",
        "additionalInformation": {
            "startOffset": start_offset,
            "eventRef": sitting.get("eventRef"),
            "sittingTitle": sitting.get("title"),
            "hlsAudioEN": (sitting.get("hlsAudioUrls") or {}).get("en") or "",
        },
    }

    record = {
        "parliament": PARLIAMENT,
        "electoralPeriod": {"number": ELECTORAL_PERIOD},
        "session": {
            "number": int(session),  # YYYYMMDD as integer (interim — see EU.md plan §scope)
            "dateStart": session_date_iso_start,
            "dateEnd": session_date_iso_end,
        },
        "agendaItem": agenda_item,
        "speechIndex": speech.get("speechIndex"),
        "originID": speech.get("speechId") or "",
        "originalLanguage": "en",
        "dateStart": speech["dateStart"],
        "dateEnd": speech["dateEnd"],
        "people": [person_record],
        "textContents": text_contents,
        "media": media_block,
        "documents": [],
        "debug": {
            "cre": {
                "speechId": speech.get("speechId"),
                "vodURL": speech.get("debug", {}).get("vodURL"),
                "annotation": speaker.get("annotation"),
                "originalSpeakerLine": speech.get("debug", {}).get("originalSpeakerLine"),
                "playerStartTime": speech.get("playerStartTime"),
                "playerEndTime": speech.get("playerEndTime"),
            },
            "merger": {
                "sittingEventRef": sitting.get("eventRef"),
                "startOffset": start_offset,
            },
            "proceedingIndex": speech.get("speechIndex"),
        },
    }
    return record


def merge_session(session: str, config, args=None) -> Path:
    """Workflow-hook entry: read intermediate proceedings + media, write merged file.

    Returns the path of the merged cache file. Signature matches
    ``WorkflowHooks.merge_session_to_file``.
    """
    proc_file = config.file(session, "proceedings")
    media_file = config.file(session, "media")
    if not proc_file.exists():
        raise FileNotFoundError(f"[{session}] proceedings intermediate missing: {proc_file}")
    if not media_file.exists():
        raise FileNotFoundError(f"[{session}] media intermediate missing: {media_file}")

    proc_doc = json.loads(proc_file.read_text())
    media_doc = json.loads(media_file.read_text())

    sittings = media_doc.get("data") or []
    if not sittings:
        raise RuntimeError(f"[{session}] no sittings in media intermediate")

    # Session date bounds from the earliest sittingStart / latest sittingEnd.
    earliest = min(s["sittingStart"] for s in sittings)
    latest = max(s["sittingEnd"] for s in sittings)
    session_iso_start = datetime.fromtimestamp(earliest, tz=timezone.utc).isoformat()
    session_iso_end = datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()

    speeches = proc_doc.get("data") or []
    records: list[dict] = []
    untimed = 0
    unmatched = 0
    duplicate = 0
    seen_ids: set[str] = set()
    for sp in speeches:
        # The CRE proceedings parser can emit the same speech twice (identical
        # speechId, text, agenda and timing — only the index differs). Each
        # speech must map to a single Stage 2 record: a duplicate would import
        # as a second clip and (sharing one vodURL) collide on sourcePage.
        sid = sp.get("speechId")
        if sid:
            if sid in seen_ids:
                duplicate += 1
                continue
            seen_ids.add(sid)
        start_epoch = _epoch(sp.get("dateStart"))
        if start_epoch is None:
            untimed += 1
            continue
        sitting = _find_sitting(start_epoch, sittings)
        if not sitting:
            unmatched += 1
            logger.debug(f"[{session}] speech {sp.get('speechId')} at "
                         f"{sp.get('dateStart')} falls outside all known sittings")
            continue
        record = _speech_record(sp, sitting, session, session_iso_start, session_iso_end)
        if record is not None:
            records.append(record)

    # Re-sequence speechIndex 1..N after dedup so the kept speeches stay
    # contiguous (dropping a duplicate would otherwise leave a gap).
    for new_index, record in enumerate(records, start=1):
        record["speechIndex"] = new_index

    logger.info(f"[{session}] merged {len(records)} speeches "
                f"({untimed} untimed/written submissions, {unmatched} outside sittings, "
                f"{duplicate} duplicate speechId(s) dropped)")

    out_path = config.file(session, "merged", create=True)
    for _s in records:
        normalize_speech_originid(_s)
    out_doc = {
        "meta": {
            "session": session,
            "parliament": PARLIAMENT,
            "electoralPeriod": ELECTORAL_PERIOD,
            "lastUpdate": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "lastProcessing": "merge",
            "processing": {
                "merge": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            },
        },
        "data": records,
    }
    out_path.write_text(json.dumps(out_doc, indent=2, ensure_ascii=False))
    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("session", type=str, help="Session key (YYYYMMDD)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Minimal local Config that finds the right paths.
    from optv.parliaments.EU.common import Config
    config = Config(args.data_dir)
    out = merge_session(args.session, config, args)
    print(out)


if __name__ == "__main__":
    main()
