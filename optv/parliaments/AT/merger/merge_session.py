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
from optv.parliaments.AT.parsers.agenda_title import split_agenda_title
from optv.parliaments.AT.scraper.fetch_session import to_roman
from optv.shared.agenda_types import classify_at, annotate_agenda_item
from optv.shared.confidence import compute_confidence
from optv.shared.meta import build_meta, fill_original_language, now_iso
from optv.shared.merge_format import split_first_last
from optv.shared.speech_id import normalize_speech_originid

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

# Chars-per-second floor for the cps-cap gate. Lower than the DE default (25000)
# because AT over-attachments (e.g. a full backbench speech bound to a short
# clip) inflate to ~10–20k chars, not the >25k whole-debate dumps DE sees.
AT_CPS_FLOOR = 8000
# Agenda types to suppress regardless of length. Empty by default: AT Q&A aligns
# healthily (unlike DE), so it is gated only by the cps-cap. Add a core type
# here (e.g. 'qa') if Whisper-QC shows it is uniformly mis-merged.
AT_BLANKET_GATE_TYPES: frozenset = frozenset()


def _text_char_count(text_contents) -> int:
    """Total sentence characters (matches whisper_diff.speech_chars / DE cps)."""
    return sum(len(sent.get("text") or "")
               for tc in text_contents or []
               for tb in tc.get("textBody") or []
               for sent in tb.get("sentences") or [])


def _trim_trailing_handoff(speech: dict, pad_intern: str | None) -> int:
    """Drop textBody blocks after the on-camera speaker's last paragraph.

    The trailing chair handoff / next-speaker intro belongs to the *next*
    speech. Conservative: only the tail is removed — leading and interior blocks
    (chair interruptions, Zwischenfragen) are kept, so a continuous speech is
    never split. No-op when ``pad_intern`` never authors a block (id mismatch):
    we keep the text verbatim rather than guess. Returns the number removed.
    """
    tcs = speech.get("textContents") or []
    if not pad_intern or not tcs:
        return 0
    tb = tcs[0].get("textBody") or []
    last = max((i for i, b in enumerate(tb)
                if str(b.get("speakerID")) == str(pad_intern)), default=None)
    if last is None or last >= len(tb) - 1:
        return 0
    tcs[0]["textBody"] = tb[:last + 1]
    # Drop any speaker now unreferenced (a chair whose only blocks were trailing);
    # always keep the on-camera speaker (first).
    refs = {str(b.get("speakerID")) for b in tcs[0]["textBody"] if b.get("speakerID")}
    refs.add(str(pad_intern))
    speech["people"] = [p for p in speech.get("people", [])
                        if str(p.get("originPersonID")) in refs] or speech.get("people", [])
    return len(tb) - (last + 1)


def _agenda_item(title: str) -> dict:
    raw = (title or "").strip()
    official, display = split_agenda_title(raw)
    item = {"officialTitle": official, "title": display}
    # Classify on the raw Mediathek title (pre-split) so signals like
    # "TOP 1 Budgetrede …" or "Erklärung der Bundesregierung" still match.
    native, core = classify_at(raw)
    annotate_agenda_item(item, native, core)
    return item


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


def apply_gate(merged: list[dict]) -> None:
    """Set ``debug.linkedMediaIndexes`` + ``debug.confidence`` (+reason) in place.

    These drive the platform text-import gate (text imports only when
    ``confidence == 1`` and ``len(linkedMediaIndexes) == 1``). Set only on
    text-bearing speeches — a media-only clip has no text to gate, and omitting
    the keys keeps the platform's ``hasAlignmentMetadata`` false for it. A stdId
    shared by >1 media clip is an ambiguous link → lmi count>1 → text gated.
    """
    std2idx: dict = {}
    for s in merged:
        if s.get("textContents"):
            std2idx.setdefault(s["debug"]["stdId"], []).append(s["speechIndex"])
    for s in merged:
        if not s.get("textContents"):
            continue
        s["debug"]["linkedMediaIndexes"] = sorted(
            std2idx.get(s["debug"]["stdId"], [s["speechIndex"]]))
        core = (s.get("agendaItem") or {}).get("type")
        chars = _text_char_count(s["textContents"])
        duration = (s.get("media") or {}).get("duration")
        conf, reason = compute_confidence(
            core, chars, duration,
            blanket_types=AT_BLANKET_GATE_TYPES, cps_floor=AT_CPS_FLOOR)
        s["debug"]["confidence"] = conf
        if reason:
            s["debug"]["confidenceReason"] = reason


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
        trimmed = _trim_trailing_handoff(speech, media_rec.get("padIntern"))
        if trimmed:
            debug["trimmedTrailingBlocks"] = trimmed
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

    apply_gate(merged)

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
