#! /usr/bin/env python3
"""Fetch the per-Sitzung agenda + speaker spine from the Plenar-TV REST API.

For each in-scope Sitzung in the archive (optionally filtered by a
``--limit-session`` regex against ``session_id``):

1. ``GET /subject/date/{meetingDate}`` → the subjects (agenda items) of that
   Sitzung (metadata only);
2. ``GET /subject/{subject_id}`` per subject → the same object **with**
   ``speakerTimings[]`` (the per-speech spine);
3. one tiny clip playlist per stream → the stream's HLS segment length, which
   decides where a per-speech clip actually starts.

The two streams (agenda + speeches) come from one source already joined by the
API, so there is no Needleman-Wunsch alignment — this is the DE-HH single-source
shape, just fetched as JSON rather than scraped from static HTML.

Jointly debated agenda items (``item.togetherStatus`` 1/2/3) list the same
speaker timings. Subjects are processed in agenda order (item number, subject
number) and each speakerTiming UUID is kept once, under the first (lowest) item
of its joint debate; that subject records the other items in
``joint_subjects`` and each emptied subject points back via ``joint_with``. The
subtitle file of a joint debate is taken from the first of its items that has
one (``vtt_subject_id`` / ``vtt_source_offset`` / ``vtt_item_start_secs``).

Writes ``original/media/{session_id}-items.json`` per Sitzung.
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
    get_segment_secs,
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
        "function": (t.get("function") or "").strip(),
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
        "together_status": item.get("togetherStatus") or 0,
        "item_start_secs": detail.get("itemStartTimeInStreamSecs"),
        "item_stop_secs": detail.get("itemStopTimeInStreamSecs"),
        "title": (detail.get("title") or "").strip(),
        "subject_art": (detail.get("subjectArt") or "").strip(),
        "consultation_type": (detail.get("consultationType") or "").strip(),
        "applicant": (detail.get("applicant") or "").strip(),
        "incoming_print": (detail.get("incomingPrint") or "").strip(),
        "incoming_print_link": (detail.get("incomingPrintLink") or "").strip(),
        "stream_file_name": (detail.get("streamFileName") or "").strip(),
        "video_start_time": video.get("startTime") or "",
        "video_offset": video.get("offset") or 0,
        "vtt_file": (detail.get("vttFile") or "").strip(),
        "vtt_offset": detail.get("vttOffset") or 0,
        "speeches": speeches,
    }


def _agenda_order(rec: dict) -> tuple:
    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return float("inf")
    return (num(rec.get("item_number")), num(rec.get("subject_number")), rec["subject_id"])


def _repair_stream_names(subjects: list[dict], session_id: str) -> None:
    """Point a subject at ``{name}_std`` when its own stream name is a stray.

    Some WP 18 subjects carry ``2018-02-28_3`` while the stream (and every other
    subject of that part) is ``2018-02-28_3_std``; the stray name does not exist
    on the VOD server.
    """
    counts: dict[str, int] = {}
    for rec in subjects:
        counts[rec["stream_file_name"]] = counts.get(rec["stream_file_name"], 0) + 1
    for rec in subjects:
        name = rec["stream_file_name"]
        if name and counts.get(name) == 1 and counts.get(f"{name}_std"):
            logger.info(f"{session_id}: subject {rec['subject_id']} stream {name!r} "
                        f"→ {name}_std")
            rec["stream_file_name"] = f"{name}_std"


def _dedup_joint_debates(subjects: list[dict]) -> None:
    """Keep each speakerTiming once, under the first subject (agenda order)."""
    keeper_of: dict[str, dict] = {}
    for rec in subjects:
        kept = []
        for sp in rec["speeches"]:
            tid = sp["timing_id"]
            keeper = keeper_of.get(tid) if tid else None
            if keeper is not None:
                rec["joint_with"] = keeper["subject_id"]
                joint = keeper.setdefault("joint_subjects", [])
                if all(j["subject_id"] != rec["subject_id"] for j in joint):
                    joint.append({
                        "subject_id": rec["subject_id"],
                        "item_number": rec["item_number"],
                        "subject_number": rec["subject_number"],
                        "title": rec["title"],
                    })
                continue
            if tid:
                keeper_of[tid] = rec
            kept.append(sp)
        rec["speeches"] = kept


def _assign_vtt_sources(subjects: list[dict]) -> None:
    """Pick the subtitle file for each subject that keeps speeches.

    A subject's own ``vttFile`` wins. Otherwise the first item of its joint
    debate that has one: the items named in ``joint_subjects`` plus the items
    flagged ``together_status`` on the same stream range.
    """
    by_id = {rec["subject_id"]: rec for rec in subjects}
    for rec in subjects:
        rec["vtt_subject_id"] = ""
        rec["vtt_source_offset"] = 0
        rec["vtt_item_start_secs"] = None
        if not rec["speeches"]:
            continue
        group = [rec] + [by_id[j["subject_id"]] for j in rec.get("joint_subjects", [])
                         if j["subject_id"] in by_id]
        if rec["together_status"]:
            span = (rec["stream_file_name"], rec["item_start_secs"], rec["item_stop_secs"])
            group += [o for o in subjects if o is not rec and o["together_status"]
                      and (o["stream_file_name"], o["item_start_secs"], o["item_stop_secs"]) == span]
        src = next((g for g in group if g.get("vtt_file")), None)
        if src is not None:
            rec["vtt_subject_id"] = src["subject_id"]
            rec["vtt_source_offset"] = src.get("vtt_offset") or 0
            rec["vtt_item_start_secs"] = src.get("item_start_secs")


def _add_segment_lengths(subjects: list[dict], session_id: str, *,
                         retry_count: int) -> None:
    lengths: dict[str, float | None] = {}
    for rec in subjects:
        name = rec["stream_file_name"]
        if name and name not in lengths:
            lengths[name] = get_segment_secs(name, retry_count=retry_count)
            if lengths[name] is None:
                logger.warning(f"{session_id}: stream {name} gives no segment length")
        rec["segment_secs"] = lengths.get(name)


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

    subjects: list[dict] = []
    for stub in listing:
        sid = stub.get("id")
        if not sid:
            continue
        detail = get_subject(sid, retry_count=retry_count)
        if detail is None:
            logger.warning(f"{entry['session_id']}: subject {sid} 404 — skipping")
            continue
        subjects.append(_subject_record(detail))

    subjects.sort(key=_agenda_order)
    _repair_stream_names(subjects, entry["session_id"])
    _dedup_joint_debates(subjects)
    _assign_vtt_sources(subjects)
    _add_segment_lengths(subjects, entry["session_id"], retry_count=retry_count)

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
