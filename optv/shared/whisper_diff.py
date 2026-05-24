#! /usr/bin/env python3
"""QC tool: compare proceedings text against faster-whisper transcription.

Three subcommands:

  rank        Rank sessions by anomaly score (chars/sec deviation, multi
              speech_id textBody chunks). Gated to sessions with cached audio.
  transcribe  Run faster-whisper (and optionally Resemblyzer change-point
              detection) on each speech of a session. Writes
              <data_dir>/cache/whisper/<session>-whisper.json. Idempotent.
  diff        Compute per-speech metrics and emit a Markdown report alongside
              a terminal summary.

Standalone — does not modify the production workflow.
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import math
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Bootstrap import path so this works as `python -m optv.shared.whisper_diff`
# and as `./optv/shared/whisper_diff.py`. Mirrors the workflow.py pattern.
if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from optv.shared import whisper_qc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def audio_path_for(data_dir: Path, period: int, session: int, speech_index: int) -> Path:
    """Match the naming convention from optv.shared.align.cachedfile."""
    fname = f"{period}{str(session).rjust(3, '0')}{speech_index}.mp3"
    return data_dir / "cache" / "audio" / fname


def whisper_path(data_dir: Path, session: str) -> Path:
    return data_dir / "cache" / "whisper" / f"{session}-whisper.json"


def report_path(data_dir: Path, session: str) -> Path:
    return data_dir / "cache" / "whisper" / f"{session}-report.md"


def load_session(data_dir: Path, session: str) -> dict:
    with open(data_dir / "processed" / f"{session}-session.json") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Per-speech text/duration extraction
# ---------------------------------------------------------------------------

def speech_chars(speech: dict) -> int:
    total = 0
    for c in speech.get("textContents", []):
        for tb in c.get("textBody", []):
            for s in tb.get("sentences", []):
                total += len(s.get("text", ""))
    return total


def speech_duration_from_alignment(speech: dict) -> float:
    times = []
    for c in speech.get("textContents", []):
        for tb in c.get("textBody", []):
            for s in tb.get("sentences", []):
                ts, te = s.get("timeStart"), s.get("timeEnd")
                if ts is not None and te is not None:
                    try:
                        times.append((float(ts), float(te)))
                    except (TypeError, ValueError):
                        pass
    if not times:
        return 0.0
    return max(t[1] for t in times) - min(t[0] for t in times)


def speech_id_count(speech: dict) -> int:
    ids = set()
    for c in speech.get("textContents", []):
        for tb in c.get("textBody", []):
            sid = tb.get("speech_id")
            if sid:
                ids.add(sid)
    return len(ids)


def comment_count(speech: dict) -> int:
    n = 0
    for c in speech.get("textContents", []):
        for tb in c.get("textBody", []):
            if tb.get("type") == "comment":
                n += 1
    return n


def speech_text(speech: dict) -> str:
    parts = []
    for c in speech.get("textContents", []):
        for tb in c.get("textBody", []):
            for s in tb.get("sentences", []):
                t = s.get("text")
                if t:
                    parts.append(t)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Rank
# ---------------------------------------------------------------------------

def session_anomaly_score(session_doc: dict, audio_dir: Path) -> dict:
    """Higher score = more suspect. Returns score + diagnostic counts."""
    speeches = session_doc.get("data", [])
    score = 0.0
    n_extreme_cps = 0
    n_multi_speech_id = 0
    n_with_audio = 0
    n_total = 0
    for s in speeches:
        n_total += 1
        period = s.get("electoralPeriod", {}).get("number")
        sess = s.get("session", {}).get("number")
        idx = s.get("speechIndex")
        if period is not None and sess is not None and idx is not None:
            ap = audio_dir / f"{period}{str(sess).rjust(3, '0')}{idx}.mp3"
            if ap.exists():
                n_with_audio += 1

        chars = speech_chars(s)
        dur = speech_duration_from_alignment(s)
        sid_n = speech_id_count(s)
        # 1–3 speech_ids per speech is normal (intro + main + transition).
        # Only count sid > 3 as pathological for diagnostic display.
        if sid_n > 3:
            n_multi_speech_id += 1
        if sid_n > 1:
            score += (sid_n - 1) * 2.0

        if dur > 0 and chars > 0:
            cps = chars / dur
            dev = abs(math.log(max(cps, 0.01) / 17.0))
            score += dev
            if cps < 8 or cps > 30:
                n_extreme_cps += 1

    coverage = n_with_audio / n_total if n_total else 0.0
    return {
        "score": score,
        "n_speeches": n_total,
        "n_extreme_cps": n_extreme_cps,
        "n_multi_speech_id": n_multi_speech_id,
        "audio_coverage": coverage,
    }


def cmd_rank(args) -> int:
    data_dir = Path(args.data_dir).resolve()
    processed = data_dir / "processed"
    audio_dir = data_dir / "cache" / "audio"
    if not processed.is_dir():
        logger.error(f"No processed/ in {data_dir}")
        return 2

    pattern = args.session_glob or "*-session.json"
    rows = []
    for f in sorted(processed.glob(pattern)):
        session = f.name.split("-")[0]
        try:
            doc = json.load(open(f))
        except json.JSONDecodeError as e:
            logger.warning(f"skip {f.name}: {e}")
            continue
        diag = session_anomaly_score(doc, audio_dir)
        if diag["audio_coverage"] < args.min_audio_coverage:
            continue
        rows.append((session, diag))

    rows.sort(key=lambda r: r[1]["score"], reverse=True)
    print(f"{'session':<8} {'score':>8} {'speeches':>9} {'multi_sid':>9} "
          f"{'extreme_cps':>11} {'audio':>6}")
    for session, d in rows[:args.top]:
        print(f"{session:<8} {d['score']:>8.1f} {d['n_speeches']:>9} "
              f"{d['n_multi_speech_id']:>9} {d['n_extreme_cps']:>11} "
              f"{d['audio_coverage']*100:>5.0f}%")
    return 0


# ---------------------------------------------------------------------------
# Transcribe
# ---------------------------------------------------------------------------

def cmd_transcribe(args) -> int:
    data_dir = Path(args.data_dir).resolve()
    out_path = whisper_path(data_dir, args.session)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    doc = load_session(data_dir, args.session)
    speeches = doc.get("data", [])
    if args.speech is not None:
        speeches = [s for s in speeches if s.get("speechIndex") == args.speech]
        if not speeches:
            logger.error(f"speechIndex {args.speech} not found in {args.session}")
            return 2

    existing = {}
    if out_path.exists() and not args.force:
        try:
            existing_doc = json.load(open(out_path))
            existing = {s["speechIndex"]: s for s in existing_doc.get("speeches", [])}
        except Exception:
            existing = {}

    results = dict(existing)
    for s in speeches:
        idx = s["speechIndex"]
        period = s["electoralPeriod"]["number"]
        sess = s["session"]["number"]
        ap = audio_path_for(data_dir, period, sess, idx)
        if not ap.exists():
            logger.warning(f"no audio for speechIndex={idx}: {ap.name}")
            continue
        if idx in results and not args.force:
            logger.info(f"speech {idx}: cached, skipping (use --force to redo)")
            continue
        logger.info(f"speech {idx}: transcribing {ap.name}...")
        t0 = time.time()
        tr = whisper_qc.transcribe_speech(ap, language=args.language,
                                          model_size=args.model,
                                          timeout=args.timeout)
        if tr is None:
            results[idx] = {"speechIndex": idx, "ok": False,
                            "audio_file": ap.name}
            continue
        entry = {
            "speechIndex": idx,
            "ok": True,
            "audio_file": ap.name,
            "transcribe_seconds": round(time.time() - t0, 1),
            **tr,
        }
        if args.with_speaker_check:
            logger.info(f"speech {idx}: speaker-change pass...")
            sp = whisper_qc.detect_speaker_changes(ap)
            if sp is not None:
                entry["speaker_check"] = sp
        results[idx] = entry

        # Persist after each speech so a crash doesn't lose work.
        out_doc = {
            "session": args.session,
            "model": args.model,
            "language": args.language,
            "with_speaker_check": args.with_speaker_check,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "speeches": [results[k] for k in sorted(results)],
        }
        with open(out_path, "w") as f:
            json.dump(out_doc, f, indent=2, ensure_ascii=False)

    logger.info(f"wrote {out_path}")
    return 0


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------

_NORM_RE = re.compile(r"[^\w\s]+", re.UNICODE)


def normalize_text(t: str) -> str:
    t = t.lower()
    t = _NORM_RE.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def text_similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, normalize_text(a), normalize_text(b),
                                   autojunk=False).ratio()


def compute_metrics(speech: dict, whisper_entry: dict | None) -> dict:
    chars_p = speech_chars(speech)
    dur = speech_duration_from_alignment(speech)
    cps_p = chars_p / dur if dur > 0 else 0.0

    text_w = (whisper_entry or {}).get("text", "") if whisper_entry else ""
    chars_w = len(text_w)
    dur_w = (whisper_entry or {}).get("duration", 0.0) if whisper_entry else 0.0
    cps_w = chars_w / dur_w if dur_w > 0 else 0.0

    sim = text_similarity(speech_text(speech), text_w) if text_w else None

    boundary = None
    if whisper_entry and whisper_entry.get("segments"):
        first_w = whisper_entry["segments"][0]["start"]
        last_w = whisper_entry["segments"][-1]["end"]
        ts, te = None, None
        for c in speech.get("textContents", []):
            for tb in c.get("textBody", []):
                for s in tb.get("sentences", []):
                    a, b = s.get("timeStart"), s.get("timeEnd")
                    if a is not None and b is not None:
                        try:
                            a, b = float(a), float(b)
                            if ts is None or a < ts: ts = a
                            if te is None or b > te: te = b
                        except (TypeError, ValueError):
                            pass
        if ts is not None and te is not None:
            boundary = max(abs(first_w - ts), abs(last_w - te))

    sp_changes = 0
    if whisper_entry and "speaker_check" in whisper_entry:
        sp_changes = len(whisper_entry["speaker_check"].get("changes", []))

    n_comments = comment_count(speech)
    sid_n = speech_id_count(speech)

    suspect_score = 0.0
    if sim is not None and sim < 0.5:
        suspect_score += (0.5 - sim) * 4
    if sp_changes > 0:
        suspect_score += sp_changes * 1.5
    if cps_p > 0 and (cps_p < 10 or cps_p > 25):
        suspect_score += abs(math.log(max(cps_p, 0.01) / 17.0))
    if boundary is not None and boundary > 5:
        suspect_score += min(boundary / 10, 3)
    if sid_n > 1:
        suspect_score += (sid_n - 1) * 0.5

    agenda = speech.get("agendaItem") or {}
    return {
        "speechIndex": speech.get("speechIndex"),
        "speaker": (speech.get("people") or [{}])[0].get("label") if speech.get("people") else None,
        "originMediaID": speech.get("originMediaID"),
        "agenda": agenda.get("title"),
        "agenda_type": agenda.get("type"),
        "agenda_native_type": agenda.get("nativeType"),
        "chars_proceedings": chars_p,
        "chars_whisper": chars_w,
        "duration_audio_seconds": round(dur, 1) if dur else None,
        "duration_whisper_seconds": round(dur_w, 1) if dur_w else None,
        "cps_proceedings": round(cps_p, 2) if cps_p else None,
        "cps_whisper": round(cps_w, 2) if cps_w else None,
        "text_similarity": round(sim, 3) if sim is not None else None,
        "boundary_divergence_s": round(boundary, 1) if boundary is not None else None,
        "speaker_change_count": sp_changes,
        "n_comments_in_proceedings": n_comments,
        "n_speech_ids_attached": sid_n,
        "suspect_score": round(suspect_score, 2),
    }


def render_terminal(rows: list[dict]) -> str:
    out = []
    out.append(f"{'idx':>4} {'cps_p':>6} {'cps_w':>6} {'sim':>5} {'spk':>3} "
               f"{'sid':>3} {'cmt':>3} {'sus':>5}  {'type':<14} speaker")
    for r in rows:
        out.append(
            f"{r['speechIndex']:>4} "
            f"{(r['cps_proceedings'] or 0):>6.1f} "
            f"{(r['cps_whisper'] or 0):>6.1f} "
            f"{(r['text_similarity'] or 0):>5.2f} "
            f"{r['speaker_change_count']:>3} "
            f"{r['n_speech_ids_attached']:>3} "
            f"{r['n_comments_in_proceedings']:>3} "
            f"{r['suspect_score']:>5.1f}  "
            f"{(r.get('agenda_type') or '-'):<14} "
            f"{(r['speaker'] or '?')[:30]}"
        )
    return "\n".join(out)


def render_type_breakdown(rows: list[dict]) -> str:
    """Group rows by agendaItem.type, summarise suspect-score / cps stats."""
    from collections import defaultdict
    import statistics
    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        buckets[r.get("agenda_type") or "(none)"].append(r)
    if not buckets:
        return ""
    rows_out = []
    rows_out.append(f"{'type':<28} {'n':>4} {'avg_sus':>8} {'p95_sus':>8} "
                    f"{'med_cps_p':>10} {'med_cps_w':>10} {'med_sim':>8}")
    by_avg = sorted(buckets.items(),
                    key=lambda kv: -statistics.mean(r["suspect_score"] for r in kv[1]))
    for tname, rs in by_avg:
        sus = [r["suspect_score"] for r in rs]
        cps_p = [r["cps_proceedings"] for r in rs if r["cps_proceedings"]]
        cps_w = [r["cps_whisper"] for r in rs if r["cps_whisper"]]
        sims = [r["text_similarity"] for r in rs if r["text_similarity"] is not None]
        avg_sus = statistics.mean(sus) if sus else 0.0
        p95 = sorted(sus)[max(int(len(sus) * 0.95) - 1, 0)] if sus else 0.0
        rows_out.append(
            f"{tname:<28} {len(rs):>4} {avg_sus:>8.1f} {p95:>8.1f} "
            f"{(statistics.median(cps_p) if cps_p else 0):>10.1f} "
            f"{(statistics.median(cps_w) if cps_w else 0):>10.1f} "
            f"{(statistics.median(sims) if sims else 0):>8.2f}"
        )
    return "\n".join(rows_out)


def render_speaker_strip(duration: float, changes: list, width: int = 60) -> str:
    if duration <= 0:
        return ""
    bar = ["-"] * width
    for ch in changes:
        t = ch.get("time_seconds", 0)
        pos = int((t / duration) * (width - 1))
        if 0 <= pos < width:
            bar[pos] = "|"
    return "".join(bar)


def render_markdown(session_doc: dict, whisper_doc: dict, rows: list[dict],
                    speeches_by_idx: dict, whisper_by_idx: dict) -> str:
    meta = session_doc.get("meta", {})
    out = []
    out.append(f"# QC report — session {meta.get('session', '?')}")
    out.append("")
    out.append(f"- Period: {session_doc['data'][0].get('electoralPeriod', {}).get('number') if session_doc.get('data') else '?'}")
    out.append(f"- Parliament: {session_doc['data'][0].get('parliament') if session_doc.get('data') else '?'}")
    out.append(f"- Date: {meta.get('dateStart', '?')} → {meta.get('dateEnd', '?')}")
    out.append(f"- Speeches: {len(rows)}")
    out.append(f"- Whisper model: {whisper_doc.get('model', '?')}")
    out.append(f"- Speaker check: {'yes' if whisper_doc.get('with_speaker_check') else 'no'}")
    out.append(f"- Generated: {whisper_doc.get('generated_at', '?')}")
    out.append("")

    out.append("## Anomalies grouped by agendaItem.type")
    out.append("")
    out.append("```")
    out.append(render_type_breakdown(rows))
    out.append("```")
    out.append("")

    out.append("## Per-speech summary")
    out.append("")
    out.append("| idx | type | speaker | cps_p | cps_w | sim | spk_chg | sid | cmt | suspect |")
    out.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        out.append(
            f"| {r['speechIndex']} "
            f"| {r.get('agenda_type') or '-'} "
            f"| {(r['speaker'] or '?')[:30]} "
            f"| {(r['cps_proceedings'] or 0):.1f} "
            f"| {(r['cps_whisper'] or 0):.1f} "
            f"| {(r['text_similarity'] or 0):.2f} "
            f"| {r['speaker_change_count']} "
            f"| {r['n_speech_ids_attached']} "
            f"| {r['n_comments_in_proceedings']} "
            f"| {r['suspect_score']:.1f} |"
        )
    out.append("")

    suspects = sorted(rows, key=lambda r: r["suspect_score"], reverse=True)[:5]
    out.append("## Top 5 suspect speeches")
    out.append("")
    for r in suspects:
        idx = r["speechIndex"]
        sp = speeches_by_idx[idx]
        we = whisper_by_idx.get(idx, {})
        out.append(f"### speechIndex {idx} — {r['speaker']} — suspect={r['suspect_score']}")
        out.append("")
        out.append(f"- originMediaID: `{r['originMediaID']}`")
        out.append(f"- agenda: {r['agenda']}")
        out.append(f"- chars: proceedings={r['chars_proceedings']}, whisper={r['chars_whisper']}")
        out.append(f"- cps: proceedings={r['cps_proceedings']}, whisper={r['cps_whisper']}")
        out.append(f"- duration (s): aligned={r['duration_audio_seconds']}, whisper={r['duration_whisper_seconds']}")
        out.append(f"- similarity: {r['text_similarity']}")
        out.append(f"- boundary divergence (s): {r['boundary_divergence_s']}")
        out.append(f"- speaker changes: {r['speaker_change_count']}, "
                   f"speech_ids attached: {r['n_speech_ids_attached']}, "
                   f"comments in proceedings: {r['n_comments_in_proceedings']}")
        out.append("")

        if we.get("speaker_check"):
            dur_w = we.get("duration", 0.0)
            strip = render_speaker_strip(dur_w, we["speaker_check"].get("changes", []))
            if strip:
                out.append("Speaker timeline (`|` = change point):")
                out.append("")
                out.append(f"```\n{strip}\n```")
                out.append("")

        ptext = speech_text(sp)
        wtext = we.get("text", "")
        ptext_short = (ptext[:1500] + "…") if len(ptext) > 1500 else ptext
        wtext_short = (wtext[:1500] + "…") if len(wtext) > 1500 else wtext
        out.append("**Proceedings text:**")
        out.append("")
        out.append("> " + ptext_short.replace("\n", "\n> "))
        out.append("")
        out.append("**Whisper transcript:**")
        out.append("")
        out.append("> " + (wtext_short or "*(empty)*").replace("\n", "\n> "))
        out.append("")
        out.append("---")
        out.append("")

    return "\n".join(out)


def cmd_diff(args) -> int:
    data_dir = Path(args.data_dir).resolve()
    session_doc = load_session(data_dir, args.session)

    wp = whisper_path(data_dir, args.session)
    if not wp.exists():
        logger.error(f"no whisper file: {wp}. Run `transcribe` first.")
        return 2
    whisper_doc = json.load(open(wp))
    whisper_by_idx = {s["speechIndex"]: s for s in whisper_doc.get("speeches", [])
                      if s.get("ok")}

    speeches = session_doc.get("data", [])
    speeches_by_idx = {s["speechIndex"]: s for s in speeches}

    rows = [compute_metrics(s, whisper_by_idx.get(s["speechIndex"])) for s in speeches]

    print(render_terminal(rows))
    print()
    print("== anomalies grouped by agendaItem.type ==")
    print(render_type_breakdown(rows))
    print()
    suspects = sorted(rows, key=lambda r: r["suspect_score"], reverse=True)[:5]
    print(f"Top suspects: {[r['speechIndex'] for r in suspects]}")

    md = render_markdown(session_doc, whisper_doc, rows, speeches_by_idx, whisper_by_idx)
    rp = report_path(data_dir, args.session)
    rp.parent.mkdir(parents=True, exist_ok=True)
    with open(rp, "w") as f:
        f.write(md)
    logger.info(f"wrote {rp}")
    return 0


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="whisper_diff")
    ap.add_argument("--debug", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_rank = sub.add_parser("rank", help="rank sessions by anomaly score")
    p_rank.add_argument("--data-dir", required=True)
    p_rank.add_argument("--top", type=int, default=10)
    p_rank.add_argument("--min-audio-coverage", type=float, default=0.5,
                        help="exclude sessions where <X fraction of speeches have cached audio")
    p_rank.add_argument("--session-glob", default=None,
                        help="glob over processed/, e.g. '21*-session.json'")
    p_rank.set_defaults(func=cmd_rank)

    p_tr = sub.add_parser("transcribe", help="run faster-whisper on a session")
    p_tr.add_argument("--data-dir", required=True)
    p_tr.add_argument("--session", required=True)
    p_tr.add_argument("--speech", type=int, default=None,
                      help="single speechIndex; default: all")
    p_tr.add_argument("--language", default="de")
    p_tr.add_argument("--model", default=whisper_qc.DEFAULT_MODEL)
    p_tr.add_argument("--timeout", type=int, default=whisper_qc.DEFAULT_TIMEOUT,
                      help="per-speech wall-clock timeout in seconds")
    p_tr.add_argument("--with-speaker-check", action="store_true",
                      help="also run Resemblyzer speaker change detection")
    p_tr.add_argument("--force", action="store_true",
                      help="redo speeches that are already in the cache")
    p_tr.set_defaults(func=cmd_transcribe)

    p_diff = sub.add_parser("diff", help="render diff report from cached whisper output")
    p_diff.add_argument("--data-dir", required=True)
    p_diff.add_argument("--session", required=True)
    p_diff.set_defaults(func=cmd_diff)

    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
