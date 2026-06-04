#! /usr/bin/env python3
"""Merge FI proceedings (PTK text) with media (broadcast speakers[]) into Stage 2.

**Media is the spine** (same model as SE/EU/NO): the broadcast ``speakers[]``
array is the canonical record of which speeches actually happened on video, and
the per-speech ``time``/``endTime`` offsets are what the platform plays. The
PTK verbatim text is grafted onto each media record.

Join key: ``personNumber`` + speech start time. The PTK
``puheenvuoroAloitusHetki`` is a naive Europe/Helsinki timestamp; the broadcast
``timeStamp`` is UTC. We convert the PTK time to UTC and, per personNumber,
match each media clip to the nearest unused PTK speech within a tolerance —
robust when a member speaks several times in one session.

Inputs:
- ``original/proceedings/{session}-proceedings.json``  (proceedings parser)
- ``original/media/{session}-media.json``              (media parser)
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.FI.merger"

from optv.parliaments.FI.common import (
    Config, parse_session_str, save_if_changed, session_number_int,
)
from optv.shared.agenda_types import annotate_agenda_item, classify_fi
from optv.shared.speech_id import normalize_speech_originid

logger = logging.getLogger(__name__)

# Max seconds between a PTK speech start and a video clip start to consider them
# the same speech (per personNumber). The two clocks agree to the second in
# practice; a generous window absorbs the occasional editorial time rounding.
JOIN_TOLERANCE_S = 300


def _helsinki_to_utc_epoch(naive_iso: Optional[str]) -> Optional[float]:
    if not naive_iso:
        return None
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Helsinki")
    except Exception:  # pragma: no cover - zoneinfo always present on 3.9+
        tz = datetime.timezone(datetime.timedelta(hours=2))
    try:
        local = datetime.datetime.fromisoformat(naive_iso).replace(tzinfo=tz)
    except ValueError:
        return None
    return local.astimezone(datetime.timezone.utc).timestamp()


def _utc_epoch(iso: Optional[str]) -> Optional[float]:
    if not iso:
        return None
    s = iso.replace("Z", "+00:00")
    try:
        return datetime.datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def _index_proceedings(proc_data: list[dict]) -> dict[int, list[dict]]:
    """``{personNumber: [proc, …]}`` with each proc tagged with its UTC epoch."""
    index: dict[int, list[dict]] = {}
    for s in proc_data:
        pn = s.get("debug", {}).get("personNumber")
        try:
            pn = int(pn)
        except (TypeError, ValueError):
            continue
        s["_utc"] = _helsinki_to_utc_epoch(s.get("dateStart"))
        s["_used"] = False
        index.setdefault(pn, []).append(s)
    return index


def _match_proceeding(media: dict, index: dict[int, list[dict]]) -> Optional[dict]:
    pn = media.get("personNumber")
    if pn is None:
        return None
    candidates = [s for s in index.get(int(pn), []) if not s["_used"]]
    if not candidates:
        return None
    media_utc = _utc_epoch(media.get("timeStamp"))
    if media_utc is None:
        chosen = candidates[0]
    else:
        scored = [(abs((s["_utc"] or 0) - media_utc), s) for s in candidates if s["_utc"]]
        if not scored:
            chosen = candidates[0]
        else:
            best_delta, chosen = min(scored, key=lambda t: t[0])
            if best_delta > JOIN_TOLERANCE_S:
                return None
    chosen["_used"] = True
    return chosen


def _media_only_people(media: dict) -> list[dict]:
    first = (media.get("firstName") or "").strip()
    last = (media.get("lastName") or "").strip()
    label = f"{first} {last}".strip() or "Tuntematon"
    person: dict[str, Any] = {"type": "memberOfParliament", "label": label,
                              "context": "main-speaker"}
    if first:
        person["firstname"] = first
    if last:
        person["lastname"] = last
    pn = media.get("personNumber")
    if pn is not None:
        person["originPersonID"] = str(pn)
    group = (media.get("party") or {}).get("fi")
    if group:
        person["faction"] = {"label": group}
    return [person]


def merge_one(media: dict, proc: Optional[dict], parliament: str,
              period_number: int, session_number: int, speech_index: int) -> dict:
    speech: dict[str, Any] = {
        "parliament": parliament,
        "electoralPeriod": {"number": period_number},
        "session": {"number": session_number},
        "speechIndex": speech_index,
        "isReply": bool(media.get("isReply")),
        "media": deepcopy(media["media"]),
    }
    debug: dict[str, Any] = {
        "personNumber": media.get("personNumber"),
        "topicId": media.get("topicId"),
    }
    if proc is not None:
        speech["agendaItem"] = deepcopy(proc["agendaItem"])
        speech["people"] = deepcopy(proc.get("people", []))
        speech["textContents"] = deepcopy(proc.get("textContents", []))
        speech["originID"] = proc.get("originID", "")
        speech["originalLanguage"] = proc.get("originalLanguage", "fi")
        if proc.get("dateStart"):
            speech["dateStart"] = proc["dateStart"]
        if proc.get("dateEnd"):
            speech["dateEnd"] = proc["dateEnd"]
        debug["proceedingIndex"] = proc.get("speechIndex")
        debug["ptkStart"] = proc.get("debug", {}).get("ptkStart")
    else:
        # Video without matching PTK text: minimal valid speech, empty text.
        title = "Täysistunto"
        agenda = {"officialTitle": title, "title": title}
        if media.get("topicId"):
            agenda["id"] = f"kohta-{media['topicId']}"
        native, core = classify_fi(title)
        annotate_agenda_item(agenda, native, core)
        speech["agendaItem"] = agenda
        speech["people"] = _media_only_people(media)
        speech["textContents"] = []
        speech["originID"] = ""
        speech["originalLanguage"] = "fi"
        debug["merge"] = {"text-missing": True}
    speech["debug"] = debug
    return speech


def merge_session(config: Config, session: str, period: int = 2023) -> dict:
    proc_path = config.file(session, "proceedings")
    media_path = config.file(session, "media")
    if not media_path.exists():
        sys.exit(f"Media JSON missing: {media_path}")

    media_doc = json.loads(media_path.read_text())
    media_data = media_doc.get("data") or []
    if not media_data:
        sys.exit(f"No media records in {media_path} — nothing to merge")

    proc_doc = json.loads(proc_path.read_text()) if proc_path.exists() else {"data": [], "meta": {}}
    proc_data = proc_doc.get("data") or []
    proc_index = _index_proceedings(proc_data)

    year, number = parse_session_str(session)
    # electoralPeriod.number is the vaalikausi (term) start year; see common.py.
    period_number = period
    session_number = session_number_int(period_number, year, number)

    merged: list[dict] = []
    matched = text_missing = 0
    for m in sorted(media_data, key=lambda r: (r.get("media", {})
                                               .get("additionalInformation", {})
                                               .get("startOffset") or 0)):
        proc = _match_proceeding(m, proc_index)
        if proc is not None:
            matched += 1
        else:
            text_missing += 1
        merged.append(merge_one(m, proc, "FI", period_number, session_number,
                                len(merged) + 1))

    proc_only = sum(1 for lst in proc_index.values() for s in lst if not s["_used"])
    if proc_only:
        logger.warning(f"{proc_only} PTK speech(es) without matching video — dropped "
                       "(no clip for the platform to render)")
    logger.info(f"Merged {len(merged)} speeches: {matched} with text, "
                f"{text_missing} media-only, {proc_only} proceedings-only dropped")

    meta_proc = proc_doc.get("meta", {})
    now = datetime.datetime.utcnow().isoformat(timespec="seconds")
    for _s in merged:
        normalize_speech_originid(_s)
    return {
        "meta": {
            "session": session,
            "schemaVersion": "1.0",
            "sourceLabel": meta_proc.get("sourceLabel", f"PTK {number}/{year} vp"),
            "dateStart": meta_proc.get("dateStart"),
            "dateEnd": meta_proc.get("dateEnd"),
            "lastUpdate": now,
            "lastProcessing": "merge",
            "processing": {
                **(meta_proc.get("processing") or {}),
                **((media_doc.get("meta") or {}).get("processing") or {}),
                "merge": now,
            },
        },
        "data": merged,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key, e.g. 2026-058")
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
