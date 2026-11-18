#! /usr/bin/env python3
"""DE-NI proceedings text from the Plenar-TV WebVTT subtitles.

Plenar-TV publishes one WebVTT file per agenda item (``GET /vtt/{subject_id}``).
Its text is the plenary protocol nearly word for word, without interjections
and without speaker labels. The web player plays the agenda item as the clip
``?start={itemStartTimeInStreamSecs}``, which the VOD server starts at a segment
boundary up to one segment early (``clip_start_secs``), and shows every cue
shifted by the item's ``vttOffset``. ``vttOffset`` is set per item to make up
for that early start (and, for some items, for a shifted subtitle file), so a
cue's stream second is ``clip_start_secs(itemStart) + vttOffset + cue``
(:func:`vtt_anchor`). Checked against Whisper on 47 agenda items of 2025 and
2026 sittings: the per-item median deviation from the audio is under 0.5 s.

Each cue goes to exactly one speech: the speech window ``[start, stop)`` that
holds the cue's midpoint, the latest-starting one where windows overlap (an
intervention inside a speech). Cues between speech windows (the chair handing
over) are dropped. Sentence timings are relative to the start of the speech's
clip, which the VOD server rounds down to a segment boundary
(``clip_start_secs``). The result is per-speech text keyed by ``speechIndex``;
the merger attaches it 1:1.

Writes ``original/proceedings/{sid}-proceedings.json``.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from ..scraper.common import clip_start_secs, fetch, vtt_uri

logger = logging.getLogger(__name__)

_TS = re.compile(r"(\d\d):(\d\d):(\d\d)[.,](\d+)\s*-->\s*(\d\d):(\d\d):(\d\d)[.,](\d+)")

# Warn when the VTT and the agenda item differ in length by more than this.
LENGTH_MISMATCH_SECS = 30.0


def _vtt_cache_dir(config) -> Path:
    d = config.dir('cache') / "vtt"
    d.mkdir(parents=True, exist_ok=True)
    return d


def fetch_vtt(subject_id: str, cache_dir: Path) -> str:
    """Cached ``GET /vtt/{subject_id}``; ``""`` when the item has no subtitles."""
    p = cache_dir / f"{subject_id}.vtt"
    if not p.exists():
        body = fetch(vtt_uri(subject_id), retry_count=5)
        if body is None:
            return ""
        p.write_bytes(body)
    txt = p.read_text(encoding="utf-8-sig")
    return txt if txt.lstrip().startswith("WEBVTT") else ""


def parse_vtt(txt: str) -> list[tuple[float, float, str]]:
    cues = []
    for blk in re.split(r"\n\n+", txt):
        lines = blk.strip().split("\n")
        ti = next((i for i, l in enumerate(lines) if "-->" in l), None)
        if ti is None:
            continue
        m = _TS.search(lines[ti])
        if not m:
            continue
        h1, m1, s1, ms1, h2, m2, s2, ms2 = map(int, m.groups())
        st = h1 * 3600 + m1 * 60 + s1 + ms1 / 1000
        en = h2 * 3600 + m2 * 60 + s2 + ms2 / 1000
        text = " ".join(l.strip() for l in lines[ti + 1:] if l.strip())
        if text:
            cues.append((st, en, text))
    return cues


def vtt_anchor(item_start_secs, vtt_offset, segment_secs=None) -> float:
    """Stream second of cue time 0 of an agenda item's VTT.

    The web player's item clip starts at ``clip_start_secs(item_start_secs)``
    and adds ``vtt_offset`` to every cue.
    """
    return clip_start_secs(item_start_secs, segment_secs) + float(vtt_offset or 0)


def assign_cues(recs: list[dict], cues: list, anchor: float) -> dict[int, list]:
    """Partition cues onto speeches → ``{speech_index: [(stream_start, stream_end, text)]}``.

    A cue belongs to the speech window ``[start_secs, stop_secs)`` holding its
    midpoint (stream time = ``anchor`` + cue time); where windows overlap, the
    latest-starting window wins. Cues outside every window are dropped.
    """
    windows = sorted(
        ((float(r["start_secs"]), float(r["stop_secs"]), r["speech_index"])
         for r in recs if r.get("start_secs") is not None and r.get("stop_secs") is not None),
        key=lambda w: (w[0], w[2]))
    out: dict[int, list] = defaultdict(list)
    for st, en, text in cues:
        a, b = anchor + st, anchor + en
        mid = (a + b) / 2
        owner = None
        for ws, we, idx in windows:
            if ws > mid:
                break
            if mid < we:
                owner = idx
        if owner is not None:
            out[owner].append((a, b, text))
    return out


def build(config, session: str) -> dict:
    media_path = config.file(session, "media")
    recs = json.loads(media_path.read_text())
    if isinstance(recs, dict):
        recs = recs.get("data", recs)
    cache_dir = _vtt_cache_dir(config)

    by_vtt: dict[str, list[dict]] = defaultdict(list)
    for r in recs:
        if r.get("vtt_subject_id") and r.get("vtt_item_start_secs") is not None:
            by_vtt[r["vtt_subject_id"]].append(r)

    assigned: dict[int, list] = {}
    anchors: dict[str, float] = {}
    for vtt_id, rs in by_vtt.items():
        cues = parse_vtt(fetch_vtt(vtt_id, cache_dir))
        if not cues:
            logger.info(f"[{session}] no subtitles for subject {vtt_id}")
            continue
        r0 = rs[0]
        anchor = vtt_anchor(r0["vtt_item_start_secs"], r0.get("vtt_offset"),
                            r0.get("segment_secs"))
        anchors[vtt_id] = anchor
        if r0.get("item_stop_secs") and r0.get("item_start_secs") is not None:
            item_len = float(r0["item_stop_secs"]) - float(r0["item_start_secs"])
            vtt_len = max(c[1] for c in cues)
            if abs(vtt_len - item_len) > LENGTH_MISMATCH_SECS:
                logger.warning(f"[{session}] subject {vtt_id}: VTT runs {vtt_len:.0f} s, "
                               f"agenda item {item_len:.0f} s")
        assigned.update(assign_cues(rs, cues, anchor))

    turns = []
    for r in sorted(recs, key=lambda r: r["speech_index"]):
        seg = assigned.get(r["speech_index"])
        if not seg:
            continue
        clip0 = r.get("clip_start_secs")
        if clip0 is None:
            clip0 = r["start_secs"]
        sentences = [{
            "text": t,
            "timeStart": f"{max(0.0, a - clip0):.3f}",
            "timeEnd": f"{max(0.0, b - clip0):.3f}",
        } for (a, b, t) in seg]
        turns.append({
            "speechIndex": r["speech_index"],
            "speaker": r.get("label", ""),
            "sentences": sentences,
        })
    return {
        "meta": {
            "session": session,
            "parliament": "DE-NI",
            "vttAnchors": anchors,
            "processing": {"parse_proceedings": datetime.utcnow().isoformat(timespec="seconds")},
        },
        "data": turns,
    }


def parse_proceedings_for_session(config, session: str) -> Path | None:
    if not config.file(session, "media").exists():
        logger.info(f"[{session}] no media — skipping VTT proceedings")
        return None
    doc = build(config, session)
    out = config.save_data(doc, session, "proceedings")
    logger.info(f"[{session}] wrote {out.name} ({len(doc['data'])} VTT turns)")
    return out


def parse_proceedings_directory(config, args=None) -> None:
    for sid in config.sessions():
        parse_proceedings_for_session(config, sid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", help="single session id (else all)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s %(message)s")
    from ..common import Config
    config = Config(args.data_dir)
    if args.session:
        parse_proceedings_for_session(config, args.session)
    else:
        parse_proceedings_directory(config, args)
