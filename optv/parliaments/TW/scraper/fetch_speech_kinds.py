#! /usr/bin/env python3
"""Fetch the per-legislator speech kinds of one plenary meeting.

Source: Legislative Yuan open data dataset 221 (委員發言名單), queried by sitting
date::

    https://data.ly.gov.tw/odw/ID221Action.action?meetingDateS=115/10/27&meetingDateE=115/11/03&fileType=json

Dates use the ROC calendar (year − 1911). Each row names a sitting day
(``smeetingDate``), the meeting (``meetingName``, e.g. ``第11屆第6會期第5次會議``),
the legislator(s) (``legislatorNameList``, joint interpellations list several
names separated by ``、``), the speech kind (``speechKindName``: 國是論壇,
法案發言, 質詢, 政黨質詢, 個人質詢, …) and the speech type (``speechType``:
口頭質詢 oral, 書面質詢 written, 聯合質詢 joint). The rows carry no time or
IVOD_ID; :mod:`..parsers.media2json` pairs them with the clips.

Output ``original/media/{session}-speechkinds.json``::

    {"meta": {"session", "meetingCode", "meetingTitle", "dates", "clipCount",
              "fetchedAt"},
     "rows": [<dataset-221 row>, ...]}   # this meeting's spoken rows, in source order

Written interpellations (``書面質詢``) are dropped: they have no clip. The file is
re-fetched when the meeting gains a sitting day, and, for up to
``RECHECK_DAYS`` after its last sitting day, while it has fewer rows than clips
(dataset 221 can lag the video by a day).
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from optv.parliaments.TW.common import Config, sessions_in_scope
from .fetch_media import today_taipei
from .ly_api import LYApiClient

logger = logging.getLogger(__name__)

DATASET_221_URL = "https://data.ly.gov.tw/odw/ID221Action.action"
WRITTEN = "書面質詢"
RECHECK_DAYS = 30


def roc_date(iso: str) -> str:
    """``2026-10-27`` → ``115/10/27``."""
    y, m, d = iso.split("-")
    return f"{int(y) - 1911}/{m}/{d}"


def iso_date(roc: str) -> str:
    """``115/10/27`` → ``2026-10-27``."""
    y, m, d = roc.strip().split("/")
    return f"{int(y) + 1911:04d}-{int(m):02d}-{int(d):02d}"


def _is_clip(ivod: dict) -> bool:
    return (ivod.get("影片種類") or "").lower() == "clip"


def fetch_speech_kinds_for_session(session: str, config: Config, client: LYApiClient,
                                   *, force: bool = False,
                                   today: date | None = None) -> Path | None:
    """Fetch dataset-221 rows for one session's sitting days.

    Returns the speech-kinds path, or ``None`` if there is nothing to fetch.
    """
    ivods_file = config.file(session, "ivods")
    if not ivods_file.exists():
        return None
    ivods_doc = json.loads(ivods_file.read_text())
    clips = [iv for iv in ivods_doc.get("ivods") or [] if _is_clip(iv)]
    dates = sorted({iv["日期"] for iv in clips if iv.get("日期")})
    if not dates:
        return None
    meeting = (clips[0].get("會議資料") or {})
    title = (meeting.get("標題") or meeting.get("會議代碼:str") or "").strip()

    out = config.file(session, "speechkinds", create=True)
    previous = json.loads(out.read_text()) if out.exists() else None
    if previous is not None and not force:
        prev_meta = previous.get("meta") or {}
        complete = len(previous.get("rows") or []) >= len(clips)
        stale_window = (today or today_taipei()) - date.fromisoformat(dates[-1]) > timedelta(days=RECHECK_DAYS)
        if prev_meta.get("dates") == dates and (complete or stale_window):
            logger.info(f"[{session}] speech kinds cached → {out.name}")
            return out

    payload = client.get_json(DATASET_221_URL, {
        "meetingDateS": roc_date(dates[0]),
        "meetingDateE": roc_date(dates[-1]),
        "fileType": "json",
    }, use_cache=False)
    wanted_dates = set(dates)
    rows = [
        row for row in payload.get("dataList") or []
        if (row.get("meetingName") or "").strip() == title
        and (row.get("speechType") or "").strip() != WRITTEN
        and iso_date(row.get("smeetingDate") or "0/1/1") in wanted_dates
    ]
    if previous is not None and previous.get("rows") == rows \
            and (previous.get("meta") or {}).get("dates") == dates:
        logger.info(f"[{session}] speech kinds unchanged ({len(rows)} rows)")
        return out

    doc = {
        "meta": {
            "session": session,
            "meetingCode": (ivods_doc.get("meta") or {}).get("meetingCode"),
            "meetingTitle": title,
            "dates": dates,
            "clipCount": len(clips),
            "fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "rows": rows,
    }
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] wrote {out.name} ({len(rows)} spoken rows for {len(clips)} clips)")
    return out


def download_speech_kinds(config: Config, args) -> None:
    """Workflow hook: fetch dataset-221 speech kinds for every in-scope session."""
    client = LYApiClient(
        retry_count=getattr(args, "retry_count", 5) or 5,
        retry_delay_max=getattr(args, "retry_delay_max", 10.0),
        legacy_tls=True,
    )
    for session in sessions_in_scope(config, args):
        try:
            fetch_speech_kinds_for_session(session, config, client, force=args.force)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[{session}] speech kinds fetch failed: "
                           f"{type(e).__name__}: {e}; agenda items fall back to the sitting title")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key (e.g. 1106005)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = Config(args.data_dir)
    client = LYApiClient(legacy_tls=True)
    fetch_speech_kinds_for_session(args.session, config, client, force=args.force)


if __name__ == "__main__":
    main()
