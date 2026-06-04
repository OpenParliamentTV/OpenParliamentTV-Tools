#! /usr/bin/env python3
"""Fetch the per-Sitzung agenda + speaker spine from the Plenar-TV REST API.

For each in-scope Sitzung in the archive (optionally filtered by a
``--limit-session`` regex against ``session_id``):

1. ``GET /subject/date/{meetingDate}`` → the subjects (agenda items) of that
   Sitzung (metadata only);
2. ``GET /subject/{subject_id}`` per subject → the same object **with**
   ``speakerTimings[]`` (the per-speech spine).

The two streams (agenda + speeches) come from one source already joined by the
API, so there is no Needleman-Wunsch alignment — this is the DE-HH single-source
shape, just fetched as JSON rather than scraped from static HTML.

Writes ``original/media/{session_id}-items.json`` per Sitzung. Speeches are
deduplicated by their speakerTiming UUID across the Sitzung (combined debates
list distinct subjects with distinct timings, but the dedup is defensive).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-NI.scraper"

from .common import (
    get_subject,
    get_subjects_by_date,
    session_page_url,
)

logger = logging.getLogger(__name__)


def _speech_from_timing(t: dict) -> dict:
    return {
        "timing_id": t.get("id") or "",
        "abg_id": t.get("abg_id"),
        "surname": (t.get("surname") or "").strip(),
        "name": (t.get("name") or "").strip(),
        "fraktion": (t.get("fraktion") or "").strip(),
        "speech_type": (t.get("speechType") or "").strip(),
        "start_secs": t.get("startTimeInStreamSecs"),
        "stop_secs": t.get("stopTimeInStreamSecs"),
    }


def _subject_record(detail: dict) -> dict:
    item = detail.get("item") or {}
    video = detail.get("video") or {}
    speeches = [
        _speech_from_timing(t)
        for t in (detail.get("speakerTimings") or [])
        if t.get("startTimeInStreamSecs") is not None
        and t.get("stopTimeInStreamSecs") is not None
    ]
    return {
        "subject_id": detail.get("id") or "",
        "subject_number": detail.get("subjectNumber"),
        "item_number": item.get("itemNumber"),
        "item_beginning": item.get("itemBeginning"),
        "title": (detail.get("title") or "").strip(),
        "subject_art": (detail.get("subjectArt") or "").strip(),
        "consultation_type": (detail.get("consultationType") or "").strip(),
        "applicant": (detail.get("applicant") or "").strip(),
        "incoming_print": (detail.get("incomingPrint") or "").strip(),
        "incoming_print_link": (detail.get("incomingPrintLink") or "").strip(),
        "stream_file_name": (detail.get("streamFileName") or "").strip(),
        "video_start_time": video.get("startTime") or "",
        "video_offset": video.get("offset") or 0,
        "vtt_offset": detail.get("vttOffset") or 0,
        "speeches": speeches,
    }


def fetch_session(entry: dict, *, retry_count: int = 20) -> dict | None:
    """Fetch all subjects (+ speaker timings) for one Sitzung."""
    date = entry.get("date")
    if not date:
        logger.warning(f"{entry['session_id']}: no meeting date in archive — skipping")
        return None

    listing = get_subjects_by_date(date, retry_count=retry_count)
    if not listing:
        logger.info(f"{entry['session_id']} ({date}): no subjects — skipping")
        return None

    seen_timings: set[str] = set()
    subjects: list[dict] = []
    for stub in listing:
        sid = stub.get("id")
        if not sid:
            continue
        detail = get_subject(sid, retry_count=retry_count)
        if detail is None:
            logger.warning(f"{entry['session_id']}: subject {sid} 404 — skipping")
            continue
        rec = _subject_record(detail)
        # Defensive global dedup by speakerTiming UUID.
        deduped = []
        for sp in rec["speeches"]:
            tid = sp["timing_id"]
            if tid and tid in seen_timings:
                continue
            if tid:
                seen_timings.add(tid)
            deduped.append(sp)
        rec["speeches"] = deduped
        subjects.append(rec)

    n_speeches = sum(len(s["speeches"]) for s in subjects)
    return {
        "session_id": entry["session_id"],
        "period": entry["period"],
        "sitzung": entry["sitzung"],
        "tagungsabschnitt": entry["tagungsabschnitt"],
        "date": date,
        "meeting_id": entry.get("meeting_id", ""),
        "session_page_url": session_page_url(
            entry["period"], entry["tagungsabschnitt"], entry["sitzung"]),
        "subjects": subjects,
        "_counts": {"subjects": len(subjects), "speeches": n_speeches},
    }


def fetch_media_for_archive(*, archive: dict, media_dir: Path,
                            force: bool = False, retry_count: int = 20,
                            session_filter: str | None = None) -> None:
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)

    for entry in archive.get("sessions", []):
        session_id = entry["session_id"]
        if session_filter and not re.match(session_filter, session_id):
            continue
        out = media_dir / f"{session_id}-items.json"
        if out.exists() and not force:
            logger.info(f"{session_id}: items.json exists — skipping (use --force to refetch)")
            continue

        try:
            payload = fetch_session(entry, retry_count=retry_count)
        except RuntimeError as ex:
            logger.warning(f"{session_id}: fetch failed ({ex}) — skipping")
            continue
        if payload is None:
            continue

        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        c = payload["_counts"]
        logger.info(f"Wrote {out.name} ({c['subjects']} subjects, "
                    f"{c['speeches']} speeches, {payload['date']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=19)
    parser.add_argument("--limit-session", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--retry-count", type=int, default=20)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    archive_path = args.data_dir / "metadata" / f"archive-wp{args.period}.json"
    with archive_path.open() as f:
        archive = json.load(f)
    fetch_media_for_archive(
        archive=archive,
        media_dir=args.data_dir / "original" / "media",
        force=args.force,
        retry_count=args.retry_count,
        session_filter=args.limit_session,
    )
