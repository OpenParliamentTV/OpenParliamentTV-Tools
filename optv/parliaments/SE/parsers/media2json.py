#! /usr/bin/env python3
"""
Build a session-keyed media JSON file from the per-debate dokumentstatus
payloads the scraper saves under ``original/media/{rel_dok_id}-debatt.json``.

Riksdag publishes one MP4 per debate (typically containing 5–40 speeches).
Per-speech navigation uses ``startpos`` (seconds offset into the debate
video) and ``anf_sekunder`` (per-speech duration). The ``anf_nummer`` field
inside each per-debate ``debatt.anforande[]`` entry matches the
protokoll-wide ``anforande_nummer`` from the speech list, so the merger
can join on ``int(anforande_nummer)`` alone.

Input:
- ``original/proceedings/{session}-anforanden.json`` — to discover which
  ``rel_dok_id`` values belong to this protokoll.
- ``original/media/{rel_dok_id}-debatt.json`` — per-debate dokumentstatus.

Output: ``original/media/{session}-media.json``
   ``{"meta": {...}, "data": [<per-speech-media-record>, ...]}``
   Each record carries ``anforande_nummer`` (the join key), ``dateStart`` /
   ``dateEnd`` derived from ``anf_klockslag`` + ``anf_sekunder``, and a
   Stage-2-shaped ``media`` block. ``videoFileURI`` / ``audioFileURI`` are
   encoded as Media Fragments URIs (``#t=start,end``) so each speech is
   uniquely addressable within the per-debate file. ``startpos`` is also
   preserved raw under ``media.additionalInformation.startOffset`` for
   downstream slicing in ``align_prep.py``.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.SE.parsers"

from optv.parliaments.SE.common import Config
from optv.parliaments import get_rights as _get_rights

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

WEBBTV_ROOT = "https://www.riksdagen.se"
SOURCE_CREATOR = _get_rights("SE", stream="media")["creator"]
SOURCE_LICENSE = _get_rights("SE", stream="media")["license"]


def _flatten(obj):
    """Riksdag's JSON sometimes wraps single items as dicts and lists as lists.
    Normalise to a list."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]


def _media_origin_id(downloadurl: str | None) -> str:
    """Extract a stable origin media id from the MP4 URL.

    Pattern: ``https://mhdownload.riksdagen.se/VOD1/HD/{ID}_720p.mp4`` →
    return ``{ID}``. Falls back to the full URL if the pattern doesn't match.
    """
    if not downloadurl:
        return ""
    base = downloadurl.rsplit("/", 1)[-1]
    # Strip any extension/quality suffix.
    for marker in ("_720p.mp4", ".mp4", "_aud.mp3"):
        if base.endswith(marker):
            return base[:-len(marker)]
    return base


def _combine_date_klockslag(date: datetime.date, klockslag: str | None) -> str | None:
    """Combine the protokoll date with a per-speech ``HH:MM:SS`` time."""
    if not klockslag:
        return None
    try:
        t = datetime.datetime.strptime(klockslag, "%H:%M:%S").time()
    except ValueError:
        return None
    return datetime.datetime.combine(date, t).isoformat(timespec="seconds")


def _add_seconds(iso_dt: str | None, seconds: int | float | None) -> str | None:
    if not iso_dt or seconds is None:
        return None
    try:
        dt = datetime.datetime.fromisoformat(iso_dt)
    except ValueError:
        return None
    return (dt + datetime.timedelta(seconds=int(seconds))).isoformat(timespec="seconds")


def build_documents(dok: dict, bilagor: list[dict]) -> list[dict]:
    """Build the ``documents`` list from a debate's ``dokument`` and ``dokbilaga.bilaga`` blocks."""
    typrubrik = dok.get("typrubrik") or dok.get("beteckning") or ""
    html_url = dok.get("dokument_url_html") or ""
    abstract = dok.get("titel") or ""
    docs: list[dict] = []
    if html_url:
        docs.append({
            "type": "officialDocument",
            "label": typrubrik,
            "sourceURI": html_url,
            "abstract": abstract,
        })
    for bilaga in bilagor:
        fil_url = bilaga.get("fil_url") or ""
        if not fil_url:
            continue
        subtitel = (bilaga.get("subtitel") or "").strip()
        label = f"{typrubrik} – {subtitel} (PDF)" if subtitel else f"{typrubrik} (PDF)"
        docs.append({
            "type": "officialDocument",
            "label": label,
            "sourceURI": fil_url,
            "abstract": abstract,
        })
    return docs


def speech_media_record(anf_entry: dict, debate_media: dict, prot_date: datetime.date,
                        documents: list[dict]) -> dict:
    """Convert one ``debatt.anforande`` entry + the debate's ``webbmedia.media``
    block into a per-speech media record.
    """
    rel_dok_id = anf_entry.get("dok_id") or ""
    try:
        anf_nummer = int(anf_entry.get("anf_nummer") or 0)
    except (TypeError, ValueError):
        anf_nummer = 0

    duration = anf_entry.get("anf_sekunder")
    try:
        duration = int(duration) if duration is not None else None
    except (TypeError, ValueError):
        duration = None

    startpos = anf_entry.get("startpos")
    try:
        startpos = int(startpos) if startpos is not None else None
    except (TypeError, ValueError):
        startpos = None

    klockslag = anf_entry.get("anf_klockslag")
    date_start = _combine_date_klockslag(prot_date, klockslag)
    date_end = _add_seconds(date_start, duration)

    download_url = debate_media.get("downloadurl") or ""
    stream_url = debate_media.get("videofileurl") or ""
    audio_url = debate_media.get("audiofileurl") or ""
    thumbnail_url = debate_media.get("thumbnailurl") or ""

    debateurl = debate_media.get("debateurl") or ""
    if debateurl.startswith("/"):
        source_page = f"{WEBBTV_ROOT}{debateurl}"
    else:
        source_page = debateurl
    if source_page and startpos is not None:
        sep = "&" if "?" in source_page else "?"
        source_page = f"{source_page}{sep}pos={startpos}&autoplay=true"
    # Two speeches can share a startpos (procedural turns at pos 0, or a reply
    # at the same offset) or have none, giving an identical sourcePage. The
    # platform keys speech identity on sourcePage, so append the per-speech
    # anförande number (unique within the protokoll) to keep them distinct.
    if source_page:
        sep = "&" if "?" in source_page else "?"
        source_page = f"{source_page}{sep}anf={anf_nummer}"

    # Riksdag publishes one MP4/MP3 per debate; per-speech navigation requires
    # a Media Fragments URI (#t=start,end) so each speech has a unique,
    # time-bounded addressable segment.
    def _with_fragment(url: str, start: int | None, dur: int | None) -> str:
        if url and start is not None and dur:
            return f"{url}#t={start},{start + dur}"
        return url

    media: dict[str, Any] = {
        "videoFileURI": _with_fragment(download_url, startpos, duration),
        "sourcePage": source_page,
        "creator": SOURCE_CREATOR,
        "license": SOURCE_LICENSE,
        "aligned": False,
    }
    if audio_url:
        media["audioFileURI"] = _with_fragment(audio_url, startpos, duration)
    if duration is not None:
        media["duration"] = duration
    origin = _media_origin_id(download_url)
    if origin:
        media["originMediaID"] = origin
    if thumbnail_url:
        media["thumbnailURI"] = thumbnail_url
        media["thumbnailCreator"] = SOURCE_CREATOR
        media["thumbnailLicense"] = SOURCE_LICENSE
    media["videoStreamURI"] = stream_url or None
    media["videoArchiveURI"] = None
    media["additionalInformation"] = {
        "startOffset": startpos,
    }

    record: dict[str, Any] = {
        "anforande_nummer": anf_nummer,
        "rel_dok_id": rel_dok_id,
        "media": media,
    }
    if documents:
        record["documents"] = documents
    if date_start:
        record["dateStart"] = date_start
    if date_end:
        record["dateEnd"] = date_end
    if anf_entry.get("debatt_titel"):
        record["debatt_titel"] = anf_entry["debatt_titel"]
    return record


def parse_media_for_session(config: Config, session: str) -> dict:
    """Build the session media JSON from on-disk per-debate files."""
    bundle_path = config.dir("proceedings") / f"{session}-anforanden.json"
    if not bundle_path.exists():
        sys.exit(f"Bundle not found: {bundle_path}")
    bundle = json.loads(bundle_path.read_text())

    prot = bundle["protokoll"]
    prot_date = datetime.datetime.strptime(prot["datum"], "%Y-%m-%d %H:%M:%S").date()

    rel_dok_ids = sorted({
        a.get("rel_dok_id") for a in bundle["anforanden"]
        if a.get("rel_dok_id")
    })
    logger.info(f"Reading {len(rel_dok_ids)} per-debate file(s) for {session}")

    records: list[dict] = []
    for rel_id in rel_dok_ids:
        path = config.dir("media") / f"{rel_id}-debatt.json"
        if not path.exists():
            logger.warning(f"  missing: {path.name} — skipping (run scraper first)")
            continue
        debate = json.loads(path.read_text())
        debate_media_list = _flatten(debate.get("webbmedia", {}).get("media"))
        debate_media = debate_media_list[0] if debate_media_list else {}
        documents = build_documents(
            debate.get("dokument", {}),
            _flatten(debate.get("dokbilaga", {}).get("bilaga")),
        )
        for anf in _flatten(debate.get("debatt", {}).get("anforande")):
            records.append(speech_media_record(anf, debate_media, prot_date, documents))

    records.sort(key=lambda r: r["anforande_nummer"])
    return {
        "meta": {
            "session": session,
            "processing": {
                "parse_media": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": records,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path,
                        help="OpenParliamentTV-Data-SE root directory")
    parser.add_argument("--session", required=True,
                        help="Session string (e.g. 2025-091)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = Config(args.data_dir)
    doc = parse_media_for_session(config, args.session)
    out = config.file(args.session, "media", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} media records)")


if __name__ == "__main__":
    main()
