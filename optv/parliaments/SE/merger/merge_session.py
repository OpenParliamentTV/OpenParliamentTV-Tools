#! /usr/bin/env python3
"""
Merge a parsed SE proceedings file with the parsed media file into Stage 2.

Architecture mirrors the German Bundestag pipeline: **media is the spine**.
Riksdag publishes one MP4 per debate with per-speech ``startpos`` /
``anf_sekunder`` / ``anf_klockslag``; that is the canonical record of which
speeches actually happened on video. Proceedings text (from
``anforande/{id}.json``) is grafted onto each media entry.

Join key: the protokoll-wide ``anforande_nummer`` integer. The per-debate
dokumentstatus's ``anf_nummer`` field carries the same numbering, so a
single integer key is sufficient (verified 2026-04-30).

Outputs ``cache/merged/{session}-merged.json`` in the Stage 2 ``{meta,
data: [speech]}`` shape required by ``optv/shared/schema/stage2-full.schema.json``.
Speeches whose media record has no matching proceedings entry are kept
with empty ``textContents`` / ``people`` and a ``debug.merge.text-missing``
marker — the platform team prefers media without text over silently
dropped video.

Inputs:
- ``original/proceedings/{session}-proceedings.json``  (proceedings parser output)
- ``original/media/{session}-media.json``              (media parser output)
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
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.SE.merger"

from optv.parliaments.SE.common import Config, save_if_changed
from optv.shared.speech_id import normalize_speech_originid

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))


def _index_proceedings(proceedings_data: list[dict]) -> dict[int, dict]:
    """Build ``{anforande_nummer: speech}`` from parsed proceedings.

    The proceedings parser sets ``speechIndex`` to ``anforande_nummer``;
    we read both fields tolerantly.
    """
    index: dict[int, dict] = {}
    for s in proceedings_data:
        try:
            n = int(s.get("speechIndex") or s.get("debug", {}).get("anforande_nummer") or 0)
        except (TypeError, ValueError):
            continue
        if n <= 0:
            continue
        if n in index:
            logger.warning(f"duplicate anforande_nummer={n} in proceedings; keeping first")
        else:
            index[n] = s
    return index


def merge_one(media_record: dict, proceeding: dict | None,
              parliament: str, period_number: int, session_number: int) -> dict:
    """Build one merged speech. ``proceeding`` is None when no text exists."""
    anf_nummer = int(media_record["anforande_nummer"])
    speech: dict = {
        "parliament": parliament,
        "electoralPeriod": {"number": period_number},
        "session": {"number": session_number},
        "speechIndex": anf_nummer,
        "media": deepcopy(media_record["media"]),
    }
    if media_record.get("dateStart"):
        speech["dateStart"] = media_record["dateStart"]
    if media_record.get("dateEnd"):
        speech["dateEnd"] = media_record["dateEnd"]

    debug: dict = {
        "anforande_nummer": anf_nummer,
        "mediaIndex": anf_nummer,
    }
    if media_record.get("debatt_titel"):
        debug["debatt_titel"] = media_record["debatt_titel"]

    rel_dok_id = media_record.get("rel_dok_id", "")

    if proceeding is None:
        # Media without proceedings: minimal valid speech with empty text.
        # ``agendaItem`` and ``people`` are required-by-schema fields; we
        # synthesize what we can from the debate title.
        title = media_record.get("debatt_titel") or ""
        speech["agendaItem"] = {"officialTitle": title, "title": title}
        speech["people"] = []
        speech["textContents"] = []
        speech["originID"] = ""
        speech["isReply"] = False
        debug["merge"] = {"text-missing": True}
    else:
        # Proceedings-driven fields (text, agenda, speaker) win — they are
        # what the platform displays.
        speech["agendaItem"] = deepcopy(proceeding["agendaItem"])
        speech["originID"] = proceeding.get("originID", "")
        speech["isReply"] = proceeding.get("isReply", False)
        speech["people"] = deepcopy(proceeding.get("people", []))
        speech["textContents"] = deepcopy(proceeding.get("textContents", []))
        debug["proceedingIndex"] = proceeding.get("speechIndex", anf_nummer)
        if proceeding.get("debug"):
            # Preserve scraper / parser breadcrumbs.
            for k, v in proceeding["debug"].items():
                debug.setdefault(k, v)

    if rel_dok_id:
        speech["agendaItem"]["id"] = rel_dok_id
    if media_record.get("documents"):
        speech["documents"] = deepcopy(media_record["documents"])

    speech["debug"] = debug
    return speech


def merge_session(config: Config, session: str) -> dict:
    proc_path = config.file(session, "proceedings")
    media_path = config.file(session, "media")
    if not proc_path.exists():
        sys.exit(f"Proceedings JSON missing: {proc_path}")
    if not media_path.exists():
        sys.exit(f"Media JSON missing: {media_path}")

    proceedings_doc = json.loads(proc_path.read_text())
    media_doc = json.loads(media_path.read_text())

    proceedings_data = proceedings_doc.get("data") or []
    media_data = media_doc.get("data") or []
    if not media_data:
        sys.exit(f"No media records in {media_path} — nothing to merge")

    proc_index = _index_proceedings(proceedings_data)

    # Read parliament/period/session metadata from any proceedings entry
    # (they all share these). Fall back to parsing the session string for
    # the "media without any matching proceedings" edge case.
    if proceedings_data:
        first = proceedings_data[0]
        parliament = first["parliament"]
        period_number = first["electoralPeriod"]["number"]
        session_number = first["session"]["number"]
    else:
        parliament = "SE"
        period_str, sess_str = session.split("-", 1)
        period_number = int(period_str)
        session_number = int(sess_str)

    merged: list[dict] = []
    matched = 0
    text_missing = 0
    for m in sorted(media_data, key=lambda r: int(r["anforande_nummer"])):
        anf_nummer = int(m["anforande_nummer"])
        proc = proc_index.get(anf_nummer)
        if proc is not None:
            matched += 1
        else:
            text_missing += 1
        merged.append(merge_one(m, proc, parliament, period_number, session_number))

    media_keys = {int(m["anforande_nummer"]) for m in media_data}
    proc_only = sorted(k for k in proc_index if k not in media_keys)
    if proc_only:
        logger.warning(
            f"{len(proc_only)} proceedings entry/entries without matching media — "
            f"dropped (anforande_nummer={proc_only[:10]}{'…' if len(proc_only) > 10 else ''}). "
            "Without video the platform can't render them; refetch media if this is unexpected."
        )

    logger.info(
        f"Merged {len(merged)} speeches: {matched} with text, "
        f"{text_missing} media-only, {len(proc_only)} proceedings-only dropped"
    )

    # Session-level dateStart/dateEnd from the proceedings parser if available,
    # otherwise computed from media records.
    proc_meta = proceedings_doc.get("meta", {})
    media_meta = media_doc.get("meta", {})
    date_start = proc_meta.get("dateStart")
    date_end = proc_meta.get("dateEnd")
    if not date_start and merged:
        date_start = next((s.get("dateStart") for s in merged if s.get("dateStart")), None)
    if not date_end and merged:
        date_end = next((s.get("dateEnd") for s in reversed(merged) if s.get("dateEnd")), None)

    for _s in merged:
        normalize_speech_originid(_s)
    return {
        "meta": {
            "session": session,
            "schemaVersion": "1.0",
            "dateStart": date_start,
            "dateEnd": date_end,
            "lastUpdate": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            "lastProcessing": "merge",
            "processing": {
                **(proc_meta.get("processing") or {}),
                **(media_meta.get("processing") or {}),
                "merge": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": merged,
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
    doc = merge_session(config, args.session)
    out = config.file(args.session, "merged", create=True)
    if save_if_changed(doc, out):
        logger.info(f"Wrote {out}")
    else:
        logger.info(f"No content change; left {out} untouched")


if __name__ == "__main__":
    main()
