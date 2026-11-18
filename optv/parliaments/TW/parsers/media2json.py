#! /usr/bin/env python3
"""Parse the raw IVOD index into per-speech media records (intermediate JSON).

Input:  ``original/media/{session}-ivods.json``  — fetch_media scraper output
Output: ``original/media/{session}-media.json``  — one record per non-``Full``
                                                   IVOD, Stage-2-shaped media block

The session's parliament/term/session_period/meeting_number metadata is taken
from the input's ``meta`` block (set by the scraper). Per-IVOD video metadata
maps directly:

================  ===============================
LY API field       Stage 2 / intermediate
================  ===============================
IVOD_ID            originID (and join key)
IVOD_URL           media.sourcePage
video_url          media.videoFileURI (HLS)
影片長度 "HH:MM:SS"  media.duration (seconds)
開始時間 / 結束時間  dateStart / dateEnd
委員名稱            people[0].label
================  ===============================

Speech kinds: if ``original/media/{session}-speechkinds.json`` (open data
dataset 221, see :mod:`..scraper.fetch_speech_kinds`) exists, each clip gets a
``speechKind`` (國是論壇, 法案發言, 質詢, …), which the merger turns into the
speech's ``agendaItem``. Dataset 221 has no time or IVOD_ID, so rows are
paired by (sitting date, legislator): when a legislator speaks more than once
on a day, the k-th row for that legislator and day goes to their k-th clip by
start time (the row order follows the order of speaking).
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
import sys
from collections import deque
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))                          # TW/
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))     # repo root
    __package__ = "optv.parliaments.TW.parsers"

from optv.parliaments.TW.common import Config
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__)

PARLIAMENT_CODE = "TW"
SOURCE_CREATOR = _get_rights("TW", stream="media")["creator"]
SOURCE_LICENSE = _get_rights("TW", stream="media")["license"]
_DURATION_RE = re.compile(r"^(\d+):(\d{2}):(\d{2})$")


def parse_duration_hhmmss(value: str | int | float | None) -> int | None:
    """``"00:30:44"`` → 1844 seconds. Falls through for ints/floats already."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    m = _DURATION_RE.match(str(value).strip())
    if not m:
        return None
    h, mn, s = (int(x) for x in m.groups())
    return h * 3600 + mn * 60 + s


def _session_number(session_period: int, meeting_number: int) -> int:
    """Pack ``會期 / 會次`` into one integer ``session.number`` per Stage 2.

    The schema requires ``session.number`` to be a positive integer; we
    encode ``會期 * 1000 + 會次`` so meetings stay distinct across session
    periods within the same term.
    """
    return session_period * 1000 + meeting_number


def _is_clip(ivod: dict) -> bool:
    return (ivod.get("影片種類") or "").lower() == "clip"


def speech_media_record(ivod: dict, *, term: int, session_period: int,
                        meeting_number: int) -> dict:
    """One IVOD stub → intermediate media record.

    The IVOD_ID is preserved at top level (as the join key for the merger)
    and also embedded in ``media.originMediaID`` per the OPTV convention.
    """
    ivod_id = ivod.get("IVOD_ID")
    duration = parse_duration_hhmmss(ivod.get("影片長度"))
    date_start = ivod.get("開始時間")
    date_end = ivod.get("結束時間")

    media: dict[str, Any] = {
        "videoFileURI": ivod.get("video_url") or "",
        "sourcePage": ivod.get("IVOD_URL") or "",
        "creator": SOURCE_CREATOR,
        "license": SOURCE_LICENSE,
        "aligned": False,
    }
    if duration is not None:
        media["duration"] = duration
    if ivod_id is not None:
        media["originMediaID"] = str(ivod_id)

    record: dict[str, Any] = {
        "IVOD_ID": ivod_id,
        "parliament": PARLIAMENT_CODE,
        "electoralPeriod": {"number": term},
        "session": {"number": _session_number(session_period, meeting_number)},
        "speakerLabel": (ivod.get("委員名稱") or "").strip(),
        "media": media,
    }
    if date_start:
        record["dateStart"] = date_start
    if date_end:
        record["dateEnd"] = date_end

    # Source-side context fields kept for the merger (agenda title, meeting
    # name, sequence) so we don't have to re-derive them from the detail.
    meeting = ivod.get("會議資料") or {}
    record["meetingName"] = ivod.get("會議名稱") or meeting.get("標題") or ""
    record["meetingCode"] = meeting.get("會議代碼") or ""
    record["meetingType"] = meeting.get("種類") or ivod.get("meetingTypeName") or ""
    return record


_NAME_SPLIT_RE = re.compile(r"[、，,]")


def _norm_name(name: str) -> str:
    return re.sub(r"\s+", "", name or "")


def _roc_to_iso(roc: str) -> str:
    y, m, d = (roc or "0/1/1").strip().split("/")
    return f"{int(y) + 1911:04d}-{int(m):02d}-{int(d):02d}"


def assign_speech_kinds(records: list[dict], rows: list[dict]) -> int:
    """Set ``speechKind`` on media records from dataset-221 rows, in place.

    ``records`` must be in chronological order. Returns the number of
    records left without a speech kind.
    """
    queues: dict[tuple[str, str], deque[str]] = {}
    for row in rows:
        kind = (row.get("speechKindName") or "").strip()
        if not kind:
            continue
        day = _roc_to_iso(row.get("smeetingDate"))
        for name in _NAME_SPLIT_RE.split(row.get("legislatorNameList") or ""):
            if _norm_name(name):
                queues.setdefault((day, _norm_name(name)), deque()).append(kind)
    unmatched = 0
    for rec in records:
        key = ((rec.get("dateStart") or "")[:10], _norm_name(rec.get("speakerLabel")))
        queue = queues.get(key)
        if queue:
            rec["speechKind"] = queue.popleft()
        else:
            unmatched += 1
    return unmatched


def parse_ivod_list(doc: dict, speech_kinds: list[dict] | None = None) -> dict:
    """Convert a raw ``{meta, ivods: [...]}`` blob into the parsed media doc.

    ``speech_kinds`` are the dataset-221 rows of the meeting (optional).
    """
    meta = dict(doc.get("meta") or {})
    term = int(meta.get("term") or 0)
    sp = int(meta.get("sessionPeriod") or 0)
    mn = int(meta.get("meetingNumber") or 0)
    session_key = meta.get("session") or ""

    clips = [iv for iv in (doc.get("ivods") or []) if _is_clip(iv)]
    skipped = len(doc.get("ivods") or []) - len(clips)
    if skipped:
        logger.info(f"[{session_key}] skipping {skipped} non-clip IVOD(s)")

    records: list[dict] = []
    for iv in clips:
        records.append(speech_media_record(iv, term=term, session_period=sp,
                                           meeting_number=mn))

    # Sort by start time so the merger sees a deterministic order.
    records.sort(key=lambda r: (r.get("dateStart") or "", r.get("IVOD_ID") or 0))
    if speech_kinds is not None:
        unmatched = assign_speech_kinds(records, speech_kinds)
        if unmatched:
            logger.info(f"[{session_key}] {unmatched} of {len(records)} clip(s) "
                        "without a dataset-221 speech kind; using the sitting title")

    date_start = records[0].get("dateStart") if records else None
    date_end = max((r.get("dateEnd") for r in records if r.get("dateEnd")),
                   default=None)

    out_meta = {
        "session": session_key,
        "meetingCode": meta.get("meetingCode"),
        "processing": {
            "parse_media": datetime.datetime.utcnow().isoformat(timespec="seconds"),
        },
    }
    if date_start:
        out_meta["dateStart"] = date_start
    if date_end:
        out_meta["dateEnd"] = date_end
    return {"meta": out_meta, "data": records}


def parse_session_media(config: Config, session: str) -> dict:
    """Read the raw IVOD list and return the parsed media doc.

    Caller writes the file (so workflow.py controls mtime semantics).
    """
    raw_path = config.file(session, "ivods")
    if not raw_path.exists():
        raise FileNotFoundError(f"No raw IVOD index at {raw_path}")
    kinds_path = config.file(session, "speechkinds")
    speech_kinds = None
    if kinds_path.exists():
        speech_kinds = json.loads(kinds_path.read_text()).get("rows") or []
    return parse_ivod_list(json.loads(raw_path.read_text()), speech_kinds)


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
    doc = parse_session_media(config, args.session)
    out = config.file(args.session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} records)")


if __name__ == "__main__":
    main()
