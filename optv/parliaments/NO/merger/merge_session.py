#! /usr/bin/env python3
"""Merge a parsed NO proceedings file with the Qbrick parts metadata.

Each speech in the proceedings carries ``dateStart`` (the meeting date +
``[HH:MM:SS]`` from the ``<Navn>`` element, naive Europe/Oslo). For every
speech we:

  1. Find the containing video part by interpreting ``dateStart`` as
     Europe/Oslo and matching against ``[tc_in_utc, tc_in_utc + duration]``.
  2. Compute ``startOffset = dateStart_utc - tc_in_utc`` (seconds, float).
  3. Estimate per-speech duration from the next speech's start (or the
     remainder of the part for the last speech). This is approximate; the
     aligner refines the end timestamp from the audio.
  4. Build the Stage-2 ``media`` block with a Media Fragment URI
     ``mp4_url#t=start,end`` (SE pattern), plus the raw ``mp4_url`` under
     ``additionalInformation`` for ``align_prep.py``.

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

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.merger"

from optv.parliaments.NO.common import Config, save_if_changed
from optv.shared.speech_id import normalize_speech_originid

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

# Europe/Oslo is CET/CEST: UTC+1 winter, UTC+2 summer. Norway observes DST
# (last Sunday March → last Sunday October). For dates after 2007 we can use
# the zoneinfo module reliably.
TZ_OSLO = datetime.timezone(datetime.timedelta(hours=1))  # fallback


def _to_utc(naive_oslo: str) -> datetime.datetime:
    """Interpret an ISO ``YYYY-MM-DDTHH:MM:SS`` as Europe/Oslo and convert to UTC."""
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Oslo")
    except Exception:
        tz = TZ_OSLO  # fallback, not DST-aware
    local = datetime.datetime.fromisoformat(naive_oslo).replace(tzinfo=tz)
    return local.astimezone(datetime.timezone.utc)


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

    # Sort speeches list itself to keep document order — we'll write the
    # merged output in clock-order (this matches platform expectations).
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
            # Per-speech duration: next speech start, else end of part, capped.
            nxt = next_start.get(id(s))
            part_end_offset = part.get("duration_seconds") or 0
            if nxt:
                duration = (nxt - utc).total_seconds()
                # Clamp into the part's window.
                duration = max(1.0, min(duration, part_end_offset - start_offset))
            else:
                duration = max(1.0, part_end_offset - start_offset)
            mp4 = part.get("mp4_url") or ""
            mfi = (f"{mp4}#t={start_offset:.3f},{start_offset + duration:.3f}"
                   if mp4 else "")
            # One video per møte-part serves many speeches, so append the
            # per-speech start offset: the platform keys speech identity on
            # sourcePage and a per-part URL would collapse every speech in the
            # part into one at import. (Also the player's seek position.)
            source_page = (f"https://www.stortinget.no/no/Hva-skjer-pa-Stortinget/"
                           f"videoarkiv/Arkiv-TV-sendinger/?meid={moteid}&del={part['delnr']}"
                           f"&t={int(start_offset)}")
            media: dict = {
                "videoFileURI": mfi,
                "sourcePage": source_page,
                "creator": "Stortinget",
                "license": "https://www.stortinget.no/no/Stottemeny/Hjelp/Nett-TV/",
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
                media["thumbnailCreator"] = "Stortinget"
                media["thumbnailLicense"] = media["license"]
            speech["media"] = media
            # Keep dateEnd naive-Oslo to mirror the dateStart format from
            # the proceedings parser. (Schema accepts either.)
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo("Europe/Oslo")
            except Exception:
                tz = TZ_OSLO
            end_local = (utc + datetime.timedelta(seconds=duration)).astimezone(tz).replace(tzinfo=None)
            speech["dateEnd"] = end_local.isoformat(timespec="seconds")
            matched_media += 1
        else:
            debug["merge"] = {"media-missing": True}
            # Minimal media stub; merged speeches without media won't align but
            # the platform can still display the text.
            speech["media"] = {
                "videoFileURI": "",
                "sourcePage": (f"https://www.stortinget.no/no/Hva-skjer-pa-Stortinget/"
                               f"videoarkiv/Arkiv-TV-sendinger/?meid={moteid}"),
                "creator": "Stortinget",
                "license": "https://www.stortinget.no/no/Stottemeny/Hjelp/Nett-TV/",
                "aligned": False,
            }
        merged.append(speech)

    logger.info(f"[{session}] merged {len(merged)} speeches: "
                f"{matched_media} with media, {len(merged) - matched_media} without")

    meta_proc = proceedings_doc.get("meta", {})
    meta_media = media_doc.get("meta", {})
    for _s in merged:
        normalize_speech_originid(_s)
    return {
        "meta": {
            "session": session,
            "schemaVersion": "1.0",
            "dateStart": meta_proc.get("dateStart"),
            "dateEnd": meta_proc.get("dateEnd"),
            "lastUpdate": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            "lastProcessing": "merge",
            "processing": {
                **(meta_proc.get("processing") or {}),
                **(meta_media.get("processing") or {}),
                "merge": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
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
