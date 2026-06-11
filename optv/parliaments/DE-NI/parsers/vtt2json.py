#! /usr/bin/env python3
"""DE-NI proceedings text from the Plenar-TV WebVTT subtitles.

DE-NI has a PDF protocol, but the Plenar-TV API also exposes time-aligned WebVTT per
subject (``GET /vtt/{subject_id}``). The per-speech media spine carries
``subject_id`` + meeting-relative ``start_secs``/``stop_secs``; each subject VTT
is calibrated onto that spine (see :func:`calibrate`) and its cues are assigned
to the speech whose window contains them. The result is per-speech text already
keyed by ``speechIndex`` — the merger attaches it 1:1, no NW join.

Writes ``original/proceedings/{sid}-proceedings.json``.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-NI.parsers"

logger = logging.getLogger(__name__)

API = "https://api.plenartv.de/vtt"
_TS = re.compile(r"(\d\d):(\d\d):(\d\d)[.,](\d+)\s*-->\s*(\d\d):(\d\d):(\d\d)[.,](\d+)")


def _vtt_cache_dir(config) -> Path:
    d = config.dir('cache') / "vtt"
    d.mkdir(parents=True, exist_ok=True)
    return d


def fetch_vtt(subject_id: str, cache_dir: Path) -> str:
    """Cached ``GET /vtt/{subject_id}`` via curl (avoids system-python SSL issues)."""
    p = cache_dir / f"{subject_id}.vtt"
    if not p.exists():
        r = subprocess.run(["curl", "-s", "-m", "40", f"{API}/{subject_id}"],
                           capture_output=True, text=True)
        p.write_text(r.stdout, encoding="utf-8")
    return p.read_text(encoding="utf-8-sig")


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


def _pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return 0.0
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy = sum((y - my) ** 2 for y in ys) ** 0.5
    return num / (dx * dy) if dx and dy else 0.0


def calibrate(rs: list[dict], cues: list) -> float:
    """Find each subject-VTT's offset into meeting time (``clip_start``).

    The cue timestamps are subject-relative (start ~0); meeting time = cue +
    clip_start. Sweep the valid range ``[max_stop − vtt_len, min_start]`` and pick
    the offset maximising the correlation between each speech's duration and the
    amount of VTT text it receives — robust to VTT preamble/gaps/closing (a fixed
    end-anchor was off by hundreds of seconds on 19079)."""
    mn = min(r["start_secs"] for r in rs)
    mx = max(r["stop_secs"] for r in rs)
    lo = mx - max(c[1] for c in cues)
    hi = mn
    if len(rs) < 3 or hi - lo < 8:
        return lo
    durs = [r["stop_secs"] - r["start_secs"] for r in rs]
    best = (-1.0, lo)
    cs = lo
    while cs <= hi + 1:
        lens = []
        for r in rs:
            a, b = r["start_secs"] - cs, r["stop_secs"] - cs
            lens.append(sum(len(t) for (st, en, t) in cues if a - 0.5 <= st < b))
        c = _pearson(durs, lens)
        if c > best[0]:
            best = (c, cs)
        cs += 5
    return best[1]


def build(config, session: str) -> dict:
    media_path = config.file(session, "media")
    recs = json.loads(media_path.read_text())
    if isinstance(recs, dict):
        recs = recs.get("data", recs)
    cache_dir = _vtt_cache_dir(config)

    by_subj: dict[str, list[dict]] = defaultdict(list)
    for r in recs:
        by_subj[r["subject_id"]].append(r)

    clip_start: dict[str, float] = {}
    cues_of: dict[str, list] = {}
    for sid, rs in by_subj.items():
        cues = parse_vtt(fetch_vtt(sid, cache_dir))
        cues_of[sid] = cues
        clip_start[sid] = calibrate(rs, cues) if cues else 0.0

    turns = []
    for r in sorted(recs, key=lambda r: r["speech_index"]):
        sid = r["subject_id"]
        cs = clip_start[sid]
        a, b = r["start_secs"] - cs, r["stop_secs"] - cs
        seg = [(st, en, t) for (st, en, t) in cues_of[sid] if a - 0.5 <= st < b]
        # Clip-relative timings as numericStrings (schema convention; the speech
        # starts at start_secs in meeting time, cue meeting-time is st + cs).
        sentences = [{
            "text": t,
            "timeStart": f"{max(0.0, st + cs - r['start_secs']):.3f}",
            "timeEnd": f"{max(0.0, en + cs - r['start_secs']):.3f}",
        } for (st, en, t) in seg]
        if not sentences:
            continue
        turns.append({
            "speechIndex": r["speech_index"],
            "speaker": r.get("label", ""),
            "sentences": sentences,
        })
    return {
        "meta": {
            "session": session,
            "parliament": "DE-NI",
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
