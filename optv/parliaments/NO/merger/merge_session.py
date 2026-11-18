#! /usr/bin/env python3
"""Merge a parsed NO proceedings file with the Qbrick parts metadata.

Each speech in the proceedings carries ``dateStart`` (the meeting date +
``[HH:MM:SS]`` from the ``<Navn>`` element, Europe/Oslo with offset). For
every speech we:

  1. Convert ``dateStart`` to UTC and find the video part whose
     ``[tc_in_utc, tc_in_utc + duration]`` window contains it.
  2. Compute ``startOffset = dateStart_utc - tc_in_utc`` (seconds, float).
  3. Take the end from the part's Qbrick chapter that starts at this offset
     (±1 s) when the part has one; otherwise from the next speech's start, or
     the end of the part for the last speech. Chapters end where the next one
     begins, so they differ from the next-speech estimate only for the last
     speech of a part. ``debug.merge.durationSource`` records which was used.
     The aligner refines the end from the audio.
  4. Build the Stage-2 ``media`` block with a Media Fragment URI
     ``mp4_url#t=start,end`` (SE pattern), a ``sourcePage`` that opens the
     archive player at the speech (``msid={start}``), plus the raw ``mp4_url``
     under ``additionalInformation`` for ``align_prep.py``.

``dateEnd`` and ``meta.dateStart``/``meta.dateEnd`` are Europe/Oslo with
offset; the meta range runs from the first speech's start to the last
speech's end.

Output: ``cache/merged/{session}-merged.json`` in the Stage 2 ``{meta, data}``
shape.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
from copy import deepcopy
from pathlib import Path
from zoneinfo import ZoneInfo

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.merger"

from optv.parliaments import get_rights as _get_rights
from optv.parliaments.NO.common import Config
from optv.shared.publish import save_if_changed
from optv.shared.speech_id import normalize_speech_originid
from optv.shared.meta import build_meta, fill_original_language, now_iso

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

MEDIA_CREATOR = _get_rights("NO", stream="media")["creator"]
MEDIA_LICENSE = _get_rights("NO", stream="media")["license"]

TZ_OSLO = ZoneInfo("Europe/Oslo")
# A chapter matches a speech when its start is this close to the speech offset.
CHAPTER_TOLERANCE = 1.0
ARCHIVE_PAGE = "https://www.stortinget.no/no/Hva-skjer-pa-Stortinget/videoarkiv/Arkiv-TV-sendinger/"


def _to_utc(iso: str) -> datetime.datetime:
    """Parse an ISO timestamp and convert it to UTC; a naive value is Europe/Oslo."""
    d = datetime.datetime.fromisoformat(iso)
    if d.tzinfo is None:
        d = d.replace(tzinfo=TZ_OSLO)
    return d.astimezone(datetime.timezone.utc)


def _oslo_iso(d: datetime.datetime) -> str:
    return d.astimezone(TZ_OSLO).isoformat(timespec="seconds")


def _chapter_at(part: dict, start_offset: float) -> dict | None:
    """The part's chapter starting at ``start_offset`` (±``CHAPTER_TOLERANCE``)."""
    for ch in part.get("chapters") or []:
        if abs(float(ch["start"]) - start_offset) <= CHAPTER_TOLERANCE:
            return ch
    return None


def _from_iso(iso_with_tz: str) -> datetime.datetime:
    d = datetime.datetime.fromisoformat(iso_with_tz)
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetime.timezone.utc)
    return d


def _assign_part(speech_utc: datetime.datetime, parts: list[dict]) -> dict | None:
    """Find the video part whose ``[tc_in_utc, tc_in_utc + duration]`` window
    contains ``speech_utc``. Returns None if none."""
    for part in parts:
        tc_in = part.get("tc_in_utc")
        dur = part.get("duration_seconds")
        if not tc_in:
            continue
        start = _from_iso(tc_in)
        end = start + datetime.timedelta(seconds=dur) if dur else None
        if end is None or end <= start:
            # No duration info — accept if speech ≥ start (last-resort fallback).
            if speech_utc >= start:
                return part
            continue
        if start <= speech_utc <= end:
            return part
    return None


def _session_range(merged: list[dict], meta_proc: dict) -> tuple[str | None, str | None]:
    """First speech start and last speech end (Europe/Oslo), else the
    proceedings meta range."""
    starts = [_to_utc(s["dateStart"]) for s in merged if s.get("dateStart")]
    ends = [_to_utc(s["dateEnd"]) for s in merged if s.get("dateEnd")] or starts
    if not starts:
        return meta_proc.get("dateStart"), meta_proc.get("dateEnd")
    return _oslo_iso(min(starts)), _oslo_iso(max(ends))


def merge_session(config: Config, session: str) -> dict:
    proc_path = config.file(session, "proceedings")
    media_path = config.file(session, "media")
    if not proc_path.exists():
        sys.exit(f"Proceedings JSON missing: {proc_path}")
    if not media_path.exists():
        sys.exit(f"Media JSON missing: {media_path}")

    proceedings_doc = json.loads(proc_path.read_text())
    media_doc = json.loads(media_path.read_text())
    speeches = proceedings_doc.get("data") or []
    parts = media_doc.get("parts") or []
    if not speeches:
        sys.exit(f"No speeches in {proc_path} — nothing to merge")
    if not parts:
        logger.warning(f"[{session}] no media parts — speeches will get empty media blocks")

    # Pre-decode speech UTC times so we can compute per-speech durations.
    moteid = media_doc.get("moteid") or int(session.split("_", 1)[1])
    for s in speeches:
        s["_utc"] = _to_utc(s["dateStart"]) if s.get("dateStart") else None

    # Sort by clock-time so per-speech duration = next_start - this_start works
    # even if the proceedings parser missed a clock anchor on one entry.
    timed = [s for s in speeches if s["_utc"] is not None]
    timed.sort(key=lambda x: x["_utc"])

    # The merged output is in clock order (platform expectation); records
    # without a clock time (only when a Referat has none at all) go last.
    speeches_sorted = list(speeches)
    speeches_sorted.sort(key=lambda x: x["_utc"] or datetime.datetime.max.replace(tzinfo=datetime.timezone.utc))

    next_start: dict[int, datetime.datetime | None] = {}
    for i, s in enumerate(timed):
        nxt = timed[i + 1]["_utc"] if i + 1 < len(timed) else None
        next_start[id(s)] = nxt

    merged: list[dict] = []
    matched_media = 0
    for i, s in enumerate(speeches_sorted, start=1):
        speech = deepcopy(s)
        speech.pop("_utc", None)
        speech["speechIndex"] = i

        utc = s["_utc"]
        part = _assign_part(utc, parts) if utc else None
        debug = speech.setdefault("debug", {})
        if utc and part:
            tc_in = _from_iso(part["tc_in_utc"])
            start_offset = (utc - tc_in).total_seconds()
            part_end_offset = part.get("duration_seconds") or 0
            tail = part_end_offset - start_offset
            chapter = _chapter_at(part, start_offset)
            nxt = next_start.get(id(s))
            if chapter is not None and float(chapter["end"]) > start_offset:
                duration = float(chapter["end"]) - start_offset
                source = "chapter"
            elif nxt:
                duration = (nxt - utc).total_seconds()
                source = "next-speech"
            else:
                duration = tail
                source = "part-end"
            # Clamp into the part's window.
            duration = max(1.0, min(duration, tail) if tail > 0 else duration)
            debug.setdefault("merge", {})["durationSource"] = source
            mp4 = part.get("mp4_url") or ""
            mfi = (f"{mp4}#t={start_offset:.3f},{start_offset + duration:.3f}"
                   if mp4 else "")
            # One video per møte-part serves many speeches, so the per-speech
            # start goes into the URL: the platform keys speech identity on
            # sourcePage, and ``msid`` makes the archive player start there
            # (it ignores ``t``).
            source_page = (f"{ARCHIVE_PAGE}?meid={moteid}&del={part['delnr']}"
                           f"&msid={int(start_offset)}")
            media: dict = {
                "videoFileURI": mfi,
                "sourcePage": source_page,
                "creator": MEDIA_CREATOR,
                "license": MEDIA_LICENSE,
                "aligned": False,
                "duration": duration,
                "originMediaID": f"{moteid}_{part['delnr']}_{i}",
                # audioFileURI omitted at merge; align_prep populates it from
                # the per-speech MP3 it slices out of the part MP4.
                "additionalInformation": {
                    "startOffset": start_offset,
                    "part": part["delnr"],
                    "qbvid": part["qbvid"],
                    "tc_in_utc": part["tc_in_utc"],
                    "mp4_url": mp4,
                    # align_prep downloads this once per part (240p, ~5 % of
                    # the 1080p platform-facing URL) — same audio track.
                    "audio_source_url": part.get("audio_mp4_url") or mp4,
                },
            }
            if part.get("thumbnail_url"):
                media["thumbnailURI"] = part["thumbnail_url"]
                media["thumbnailCreator"] = MEDIA_CREATOR
                media["thumbnailLicense"] = media["license"]
            speech["media"] = media
            speech["dateEnd"] = _oslo_iso(utc + datetime.timedelta(seconds=duration))
            matched_media += 1
        else:
            debug["merge"] = {"media-missing": True}
            # Minimal media stub; merged speeches without media won't align but
            # the platform can still display the text.
            speech["media"] = {
                "videoFileURI": "",
                "sourcePage": f"{ARCHIVE_PAGE}?meid={moteid}",
                "creator": MEDIA_CREATOR,
                "license": MEDIA_LICENSE,
                "aligned": False,
            }
        merged.append(speech)

    logger.info(f"[{session}] merged {len(merged)} speeches: "
                f"{matched_media} with media, {len(merged) - matched_media} without")

    meta_proc = proceedings_doc.get("meta", {})
    meta_media = media_doc.get("meta", {})
    for _s in merged:
        normalize_speech_originid(_s)
    fill_original_language(merged, "NO")
    date_start, date_end = _session_range(merged, meta_proc)
    return {
        "meta": build_meta(
            "NO",
            session=session,
            electoral_period=(merged[0].get("electoralPeriod") if merged else None),
            date_start=date_start,
            date_end=date_end,
            processing={
                **(meta_proc.get("processing") or {}),
                **(meta_media.get("processing") or {}),
                "merge": now_iso(),
            },
        ),
        "data": merged,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session string e.g. 22_11518")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = merge_session(config, args.session)
    out = config.file(args.session, "merged", create=True)
    if save_if_changed(doc, out):
        logger.info(f"Wrote {out}")
    else:
        logger.info(f"No content change; left {out} untouched")


if __name__ == "__main__":
    main()
