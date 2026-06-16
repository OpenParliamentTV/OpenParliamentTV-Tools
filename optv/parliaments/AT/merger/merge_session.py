#! /usr/bin/env python3
"""Merge AT media + proceedings streams into Stage 2.

**Media is the spine** (like SE/ES): the Mediathek ``redner`` list is the
canonical record of which speeches exist on camera. Proceedings text is grafted
on by an **exact id join** — the protocol ``std_id`` equals the media
``st_objekte_id`` (both surfaced as ``stdId``), so no fuzzy alignment is needed.

That same ``std_id`` is *both* the media source id and the text source id, so it
is not a distinct joint id: it goes into ``media.originMediaID`` and
``textContents[].originTextID`` and ``speech.originID`` is left unset (the
normalizer would drop a redundant copy anyway).

The on-camera speaker is identified by ``media.padIntern`` (== the protocol
header's ``PAD_<n>``); the merger guarantees that person is present in
``people`` and listed first. Speeches with no matching proceedings text are kept
as media-only records with empty ``textContents`` (the platform prefers video
without text over a dropped clip).

Outputs ``cache/merged/{session}-merged.json``.
"""

from __future__ import annotations

import argparse
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
    __package__ = "optv.parliaments.AT.merger"

from optv.parliaments.AT.common import Config, save_if_changed
from optv.parliaments.AT.parsers.media2json import parse_session as parse_media_session
from optv.parliaments.AT.parsers.proceedings2json import (
    parse_session as parse_proceedings_session, clean_person_name)
from optv.parliaments.AT.scraper.fetch_session import to_roman
from optv.shared.meta import build_meta, fill_original_language, now_iso
from optv.shared.merge_format import split_first_last
from optv.shared.speech_id import normalize_speech_originid

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))


def _agenda_item(title: str) -> dict:
    title = (title or "").strip()
    return {"officialTitle": title, "title": title}


def _order_people(people: list[dict], pad_intern: str, speaker_name: str) -> list[dict]:
    """Ensure the on-camera speaker (``pad_intern``) is present and first."""
    people = deepcopy(people or [])
    idx = next((i for i, p in enumerate(people)
                if p.get("originPersonID") == pad_intern), None)
    if idx is None:
        label = clean_person_name(speaker_name)
        first, last = split_first_last(label)
        people.insert(0, {
            "type": "memberOfParliament",
            "label": label,
            "firstname": first,
            "lastname": last,
            "context": "main-speaker",
            "originPersonID": pad_intern,
        })
    elif idx != 0:
        people.insert(0, people.pop(idx))
    return people


def merge_one(media_rec: dict, proc: dict | None, parliament: str,
              period: int, sitting: int, session_dates: tuple[str | None, str | None]) -> dict:
    speech: dict = {
        "parliament": parliament,
        "electoralPeriod": {"number": period, "label": to_roman(period)},
        "session": {"number": sitting},
        "speechIndex": media_rec["speechIndex"],
        "agendaItem": _agenda_item(media_rec.get("agendaTitle")),
        "media": deepcopy(media_rec["media"]),
    }
    if session_dates[0]:
        speech["session"]["dateStart"] = session_dates[0]
    if session_dates[1]:
        speech["session"]["dateEnd"] = session_dates[1]
    if media_rec.get("dateStart"):
        speech["dateStart"] = media_rec["dateStart"]
    if media_rec.get("dateEnd"):
        speech["dateEnd"] = media_rec["dateEnd"]

    debug = {"mediaIndex": media_rec["speechIndex"], "stdId": media_rec["stdId"]}
    if media_rec.get("debatteId") is not None:
        debug["debatteId"] = media_rec["debatteId"]

    if proc is not None:
        speech["people"] = _order_people(proc.get("people", []),
                                         media_rec.get("padIntern"),
                                         media_rec.get("speakerName", ""))
        speech["textContents"] = deepcopy(proc.get("textContents", []))
        debug["proceedingIndex"] = media_rec["stdId"]
    else:
        speech["people"] = _order_people([], media_rec.get("padIntern"),
                                         media_rec.get("speakerName", ""))
        speech["textContents"] = []
        debug["textMissing"] = True

    speech["debug"] = debug
    return speech


def merge_session(config: Config, session: str, period: int) -> dict:
    # Media is always re-parsed (cheap, and the per-speech sourcePage/dedup logic
    # lives there). Proceedings parsing runs spaCy over the whole protocol, so
    # reuse the cached parsed file when present (written by the parse stage) and
    # only fall back to a fresh parse when it's missing.
    media_doc = parse_media_session(config, session, period)
    proc_file = config.file(session, "proceedings")
    if proc_file.exists():
        proc_doc = json.loads(proc_file.read_text())
    else:
        proc_doc = parse_proceedings_session(config, session, period)

    media_data = media_doc.get("data") or []
    if not media_data:
        logger.warning(f"[{session}] no media records — empty session")
    proc_index = {p["stdId"]: p for p in (proc_doc.get("data") or [])}

    sitting = int(str(session)[len(str(period)):]) if str(session).startswith(str(period)) else int(session)
    date_start = media_doc["meta"].get("dateStart")
    date_end = media_doc["meta"].get("dateEnd")

    merged: list[dict] = []
    matched = 0
    for m in media_data:
        proc = proc_index.get(m["stdId"])
        if proc is not None and (proc.get("textContents") or [{}])[0].get("textBody"):
            matched += 1
        else:
            proc = proc if proc and proc.get("textContents") else None
        merged.append(merge_one(m, proc, "AT", period, sitting, (date_start, date_end)))

    logger.info(f"[{session}] merged {len(merged)} speeches ({matched} with text, "
                f"{len(merged) - matched} media-only)")

    for s in merged:
        normalize_speech_originid(s)
    fill_original_language(merged, "AT")

    return {
        "meta": build_meta(
            "AT",
            session=session,
            electoral_period={"number": period},
            date_start=date_start,
            date_end=date_end,
            processing={"merge": now_iso()},
        ),
        "data": merged,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key (e.g. 27144)")
    parser.add_argument("--period", type=int, default=27)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = merge_session(config, args.session, args.period)
    out = config.file(args.session, "merged", create=True)
    if save_if_changed(doc, out):
        logger.info(f"Wrote {out}")
    else:
        logger.info(f"No content change; left {out} untouched")


if __name__ == "__main__":
    main()
