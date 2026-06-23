#!/usr/bin/env python3
"""Audit published Stage-2 sessions for out-of-range alignment timecodes.

Background
----------
A speech's per-sentence timecodes can run past the media ``duration`` when the
audio that was aligned was longer than the clip the platform serves (an
untrimmed/whole-asset source, or a clip the CDN later trimmed). The platform
positions search-result hit markers as ``timecode / duration``, so an end past
the duration renders the marker outside its ``.hitTimeline`` track and breaks
the layout. Reference case: DE-0210037079 (session 21037, speech 79) — 57.96s
of timecodes on a 21s clip. See ``optv/shared/align.py``
(``aligned_end_out_of_bounds``) for the aligner-side guard that now prevents
new occurrences, and ``semantic_validator._rule_sentence_time_bounds`` for the
publish-time catch-net.

What this does
--------------
**Report only.** It scans ``<data-dir>/processed/*-session.json`` and lists the
affected speeches (with their ``agendaItem.type``) plus per-type and per-period
summaries. Optionally (``--check-cdn``) it probes each affected speech's
``audioFileURI`` to tell which can be re-aligned (audio still reachable) vs
which are on the dead legacy CDN.

What this does NOT do (yet)
---------------------------
It does not correct any data. A correct backfill must reconcile not just the
published JSON but the align/ner/merged **caches on both machines** — this dev
box AND the production host where alignment actually ran. Those cache states
diverge, so a one-shot re-align in one place would not match the other. The
correction is therefore deferred and must be coordinated across both caches:

  * Live-CDN sessions (audio reachable): re-align with
    ``optv/parliaments/<CODE>/workflow.py --align-sentences --force
    --limit-session <regex>``. Today's trimmed audio aligns in bounds, and the
    aligner's bound guard drops any persistent metadata-vs-file mismatch.
  * Dead-CDN sessions (audio gone): re-alignment is impossible; clamp in place
    or rely on the platform render clamp.
  * Catastrophic (>5x) cases: inspect by hand — several are wrong/whole-session
    media links (bad merge), not just trimming.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

logger = logging.getLogger("timecode_audit")


def speech_max_timeend(speech: dict) -> float | None:
    """Largest numeric sentence ``timeEnd`` in a speech, or None if untimed."""
    mx = None
    for tc in speech.get("textContents") or []:
        for body in tc.get("textBody") or []:
            for sent in body.get("sentences") or []:
                te = sent.get("timeEnd")
                if te in (None, ""):
                    continue
                try:
                    v = float(te)
                except (TypeError, ValueError):
                    continue
                mx = v if mx is None else max(mx, v)
    return mx


def _agenda_type(speech: dict) -> str:
    return (speech.get("agendaItem") or {}).get("type") or "(none)"


def _audio_reachable(url: str | None, timeout: float = 30.0) -> bool | None:
    """HEAD-probe an audio URL. None when no URL; bool otherwise."""
    if not url:
        return None
    try:
        out = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-I", url],
            capture_output=True, text=True, timeout=timeout).stdout.strip()
        return out.startswith("2") or out.startswith("3")
    except (subprocess.TimeoutExpired, OSError):
        return False


def audit(processed_dir: Path, tolerance: float, check_cdn: bool):
    rows = []
    affected_by_type = Counter()
    total_by_type = Counter()
    affected_sessions_by_period = defaultdict(set)
    sessions = defaultdict(list)

    for f in sorted(processed_dir.glob("*-session.json")):
        try:
            doc = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("skipping %s: %s", f.name, e)
            continue
        period = f.name[:2]
        for sp in doc.get("data") or []:
            dur = (sp.get("media") or {}).get("duration")
            if not isinstance(dur, (int, float)) or dur <= 0:
                continue
            mx = speech_max_timeend(sp)
            if mx is None:
                continue
            atype = _agenda_type(sp)
            total_by_type[atype] += 1
            if mx > dur + tolerance:
                affected_by_type[atype] += 1
                affected_sessions_by_period[period].add(f.name)
                url = (sp.get("media") or {}).get("audioFileURI")
                reachable = _audio_reachable(url) if check_cdn else None
                rows.append({
                    "session": f.name,
                    "speechIndex": sp.get("speechIndex"),
                    "agendaType": atype,
                    "duration": round(float(dur), 1),
                    "maxEnd": round(mx, 1),
                    "ratio": round(mx / dur, 2),
                    "audioReachable": reachable,
                })
                sessions[f.name].append(mx / dur)
    return rows, affected_by_type, total_by_type, affected_sessions_by_period, sessions


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", required=True, type=Path,
                   help="data root containing a processed/ subdirectory")
    p.add_argument("--tolerance", type=float, default=1.0,
                   help="seconds of slack over duration before flagging (default 1.0)")
    p.add_argument("--check-cdn", action="store_true",
                   help="HEAD-probe each affected audioFileURI (slow, network)")
    p.add_argument("--csv", type=Path, default=None,
                   help="write the per-speech rows to this CSV path")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    processed = args.data_dir / "processed"
    if not processed.is_dir():
        p.error(f"no processed/ directory under {args.data_dir}")

    rows, by_type, total_by_type, sessions_by_period, sessions = audit(
        processed, args.tolerance, args.check_cdn)

    severe = sum(1 for s in sessions.values() if max(s) >= 2)
    print(f"Affected: {len(sessions)} sessions / {len(rows)} speeches "
          f"(tolerance {args.tolerance}s over duration)")
    print(f"  severe (>=2x worst speech): {severe} sessions")

    print("\nBy electoral period (affected session count):")
    for period in sorted(sessions_by_period):
        print(f"  P{period}: {len(sessions_by_period[period])}")

    print("\nBy agendaItem.type (affected / total = rate, sorted by rate):")
    ordered = sorted(by_type, key=lambda t: by_type[t] / max(total_by_type[t], 1),
                     reverse=True)
    for t in ordered:
        c, tot = by_type[t], total_by_type[t]
        print(f"  {t:<30} {c:>5} / {tot:<6} = {100 * c / tot:5.1f}%")

    print("\nWorst 15 speeches (ratio  maxEnd  duration  session  sp#  type):")
    for r in sorted(rows, key=lambda r: r["ratio"], reverse=True)[:15]:
        reach = "" if r["audioReachable"] is None else \
            (" reachable" if r["audioReachable"] else " DEAD-CDN")
        print(f"  {r['ratio']:6.1f}x  {r['maxEnd']:8.1f}  {r['duration']:7.1f}  "
              f"{r['session']}  sp#{r['speechIndex']}  {r['agendaType']}{reach}")

    if args.csv:
        with args.csv.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else
                               ["session", "speechIndex", "agendaType",
                                "duration", "maxEnd", "ratio", "audioReachable"])
            w.writeheader()
            w.writerows(rows)
        print(f"\nWrote {len(rows)} rows to {args.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
