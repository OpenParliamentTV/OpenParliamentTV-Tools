#! /usr/bin/env python3
"""QC tool: compare proceedings text against faster-whisper transcription.

Three subcommands:

  rank        Rank sessions by anomaly score (chars/sec deviation, multi
              speech_id textBody chunks). Gated to sessions with cached audio.
  transcribe  Run faster-whisper (and optionally Resemblyzer change-point
              detection) on each speech of a session. Writes
              <data_dir>/cache/whisper/<session>-whisper.json. Idempotent: a
              transcribed speech is skipped, a failed one is tried once more.
  diff        Compute per-speech metrics and emit a Markdown report alongside
              a terminal summary, plus the metrics as
              <data_dir>/cache/whisper/<session>-qc.json.

Standalone — does not modify the production workflow.
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import math
import os
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


def qc_path(data_dir: Path, session: str) -> Path:
    return data_dir / "cache" / "whisper" / f"{session}-qc.json"


def load_session(data_dir: Path, session: str) -> dict:
    with open(data_dir / "processed" / f"{session}-session.json") as f:
        return json.load(f)


def _write_json(path: Path, doc: dict) -> None:
    """Replace ``path`` in one step, so an interrupted run never leaves half a file."""
    partial = path.with_name(path.name + ".partial")
    with open(partial, "w") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
    os.replace(partial, path)


# ---------------------------------------------------------------------------
# Per-speech text/duration extraction
# ---------------------------------------------------------------------------

def spoken_text_contents(speech: dict) -> list:
    """The text entries to hold against the audio.

    A speech that carries its text in several languages (the original and a
    rendering) is compared in the language spoken, ``originalLanguage``.
    """
    contents = speech.get("textContents") or []
    spoken = speech.get("originalLanguage")
    if spoken and len({c.get("language") for c in contents}) > 1:
        own = [c for c in contents if c.get("language") == spoken]
        if own:
            return own
    return contents


def speech_chars(speech: dict) -> int:
    total = 0
    for c in spoken_text_contents(speech):
        for tb in c.get("textBody", []):
            for s in tb.get("sentences", []):
                total += len(s.get("text", ""))
    return total


def speech_duration_from_alignment(speech: dict) -> float:
    times = []
    for c in spoken_text_contents(speech):
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
    for c in spoken_text_contents(speech):
        for tb in c.get("textBody", []):
            sid = tb.get("speech_id")
            if sid:
                ids.add(sid)
    return len(ids)


def comment_count(speech: dict) -> int:
    n = 0
    for c in spoken_text_contents(speech):
        for tb in c.get("textBody", []):
            if tb.get("type") == "comment":
                n += 1
    return n


def speech_text(speech: dict) -> str:
    parts = []
    for c in spoken_text_contents(speech):
        for tb in c.get("textBody", []):
            for s in tb.get("sentences", []):
                t = s.get("text")
                if t:
                    parts.append(t)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Which transcripts still belong to their speech
# ---------------------------------------------------------------------------

# A transcript's audio may differ from the length its speech has now by this much.
LENGTH_TOLERANCE_SECONDS = 10
LENGTH_TOLERANCE_SHARE = 0.05
# A session whose transcripts differ this often was re-merged since: all of them are outdated.
OUTDATED_SHARE = 0.10
OUTDATED_MIN = 3


def declared_duration(speech: dict) -> tuple[float, str]:
    """Seconds of audio the speech itself states, and where: ``media.duration``, the
    ``#t=start,end`` fragment of its video URI, or the stream offsets a session-stream
    parliament keeps in ``media.additionalInformation``. ``(0.0, "")`` when it states none."""
    media = speech.get("media") or {}
    try:
        duration = float(media.get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration > 0:
        return duration, "media.duration"

    from optv.shared.audio_prep import fragment_speech_audio
    fragment = fragment_speech_audio(speech, require_text=False)
    if fragment is not None and fragment.duration:
        return float(fragment.duration), "fragment"

    info = media.get("additionalInformation")
    if isinstance(info, dict):
        try:
            stop = float(info.get("stopInStreamSecs") or 0)
            start = float(info.get("clipStartInStreamSecs", info.get("startInStreamSecs")) or 0)
        except (TypeError, ValueError):
            stop = start = 0.0
        if stop > start >= 0 and stop > 0:
            return stop - start, "stream-offsets"
    return 0.0, ""


def last_sentence_end(speech: dict) -> float:
    """End time of the last aligned sentence, which lies at the end of the speech's audio."""
    latest = 0.0
    for c in speech.get("textContents") or []:
        for tb in c.get("textBody") or []:
            if not isinstance(tb, dict):
                continue
            for s in tb.get("sentences") or []:
                try:
                    latest = max(latest, float(s.get("timeEnd") or 0))
                except (TypeError, ValueError, AttributeError):
                    pass
    return latest


def expected_seconds(speech: dict) -> float:
    """Length the speech's audio should have: what the speech states, else the end of
    its last aligned sentence. ``0.0`` when neither is known."""
    return declared_duration(speech)[0] or last_sentence_end(speech)


def length_differs(actual: float, expected: float) -> bool:
    return abs(actual - expected) > max(LENGTH_TOLERANCE_SECONDS, LENGTH_TOLERANCE_SHARE * expected)


def too_many_differ(differing: int, checked: int) -> bool:
    return differing >= OUTDATED_MIN and differing / checked >= OUTDATED_SHARE


def clip_seconds(audio: Path) -> float:
    """Length of a cached clip; ``0.0`` when it cannot be measured."""
    try:
        from optv.shared.audio_prep import _duration_seconds
        return _duration_seconds(audio)
    except Exception as e:
        logger.debug(f"no duration for {audio.name}: {e}")
        return 0.0


def outdated_clips(data_dir: Path, speeches: list) -> tuple[int, int]:
    """How many cached clips differ in length from their speech, and how many could be
    compared. Clips cut before a re-merge hold another stretch of audio than the speech
    at their index now covers; transcribing them proves nothing."""
    differing = checked = 0
    for speech in speeches:
        expected = expected_seconds(speech)
        if not expected:
            continue
        actual = clip_seconds(audio_path_for(data_dir, speech["electoralPeriod"]["number"],
                                             speech["session"]["number"], speech["speechIndex"]))
        if not actual:
            continue
        checked += 1
        differing += length_differs(actual, expected)
    return differing, checked


def current_transcripts(speeches: list, whisper_doc: dict) -> tuple[dict, int]:
    """The transcripts that still belong to their speech, by ``speechIndex``, and how
    many were set aside as outdated.

    A transcript is filed under the index its speech had when it was made. After a
    re-merge that adds, drops or re-cuts speeches, index N names another stretch of
    audio. That shows in the lengths: when a tenth of a session's transcripts were made
    from audio of another length than their speech has now, none of them can be trusted
    to sit at the right index. A stray difference is a quirk of one clip and is kept.
    A session whose speeches state no length and carry no sentence times cannot be
    checked and is taken as current.
    """
    by_index = {s.get("speechIndex"): s for s in speeches}
    transcripts = {}
    outdated = checked = differing = 0
    for entry in whisper_doc.get("speeches") or []:
        if not entry.get("ok"):
            continue
        speech = by_index.get(entry.get("speechIndex"))
        if speech is None:
            outdated += 1
            continue
        transcripts[entry["speechIndex"]] = entry
        expected = expected_seconds(speech)
        if expected and entry.get("duration"):
            checked += 1
            differing += length_differs(float(entry["duration"]), expected)
    if too_many_differ(differing, checked):
        return {}, outdated + len(transcripts)
    return transcripts, outdated


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
        session = f.name.removesuffix("-session.json")
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

# Stage 2 language codes Whisper names differently.
WHISPER_LANGUAGES = {"nb": "no"}
# A speech that failed this often is left alone until --force.
MAX_ATTEMPTS = 2
# Shortest per-speech time limit, and how many times a clip's length it may take.
MIN_TIMEOUT = 600
TIMEOUT_FACTOR = 3
# Exit code of `transcribe` when the cached clips no longer belong to the speeches.
EXIT_OUTDATED_CLIPS = 3


def whisper_language(speech: dict, override: str | None = None) -> str | None:
    """The language to transcribe a speech in: ``override``, else the speech's
    ``originalLanguage``. ``None`` lets Whisper detect it."""
    language = override or speech.get("originalLanguage")
    return WHISPER_LANGUAGES.get(language, language) or None


def speech_timeout(audio: Path) -> int:
    """Time limit for one clip: three times its length, within ten minutes and
    the flat default. A deadlocked transcription then costs minutes, not hours."""
    duration = clip_seconds(audio)
    if not duration:
        return whisper_qc.DEFAULT_TIMEOUT
    return int(max(MIN_TIMEOUT, min(TIMEOUT_FACTOR * duration, whisper_qc.DEFAULT_TIMEOUT)))


def cmd_transcribe(args) -> int:
    data_dir = Path(args.data_dir).resolve()
    out_path = whisper_path(data_dir, args.session)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    doc = load_session(data_dir, args.session)
    speeches = doc.get("data", [])

    existing = {}
    if out_path.exists() and not args.force:
        try:
            existing_doc = json.load(open(out_path))
            existing = {s["speechIndex"]: s for s in existing_doc.get("speeches", [])}
        except Exception:
            existing_doc, existing = {}, {}
        # Transcripts made before a re-merge no longer sit at their speech's index.
        current, outdated = current_transcripts(speeches, existing_doc)
        if outdated:
            logger.warning(f"{outdated} transcripts no longer match their speech; transcribing those again")
            existing = {idx: s for idx, s in existing.items() if not s.get("ok") or idx in current}

    if args.speech is not None:
        speeches = [s for s in speeches if s.get("speechIndex") == args.speech]
        if not speeches:
            logger.error(f"speechIndex {args.speech} not found in {args.session}")
            return 2

    if not args.force:
        differing, checked = outdated_clips(data_dir, speeches)
        if too_many_differ(differing, checked):
            logger.error(f"{differing} of {checked} cached clips differ in length from their speech: "
                         "they were cut before the session was merged anew. Stage the audio again "
                         "(or pass --force to transcribe them as they are).")
            return EXIT_OUTDATED_CLIPS

    def save() -> None:
        # Persist after each speech so a crash doesn't lose work.
        _write_json(out_path, {
            "session": args.session,
            "model": args.model,
            "language": args.language or "per speech",
            "with_speaker_check": args.with_speaker_check,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "speeches": [results[k] for k in sorted(results)],
        })

    results = dict(existing)
    transcribed = failed = 0
    for s in speeches:
        idx = s["speechIndex"]
        period = s["electoralPeriod"]["number"]
        sess = s["session"]["number"]
        ap = audio_path_for(data_dir, period, sess, idx)
        if not ap.exists():
            logger.warning(f"no audio for speechIndex={idx}: {ap.name}")
            continue
        previous = results.get(idx) if not args.force else None
        attempts = 0
        if previous is not None:
            if previous.get("ok"):
                logger.info(f"speech {idx}: cached, skipping (use --force to redo)")
                continue
            attempts = previous.get("attempts", 1)
            if attempts >= MAX_ATTEMPTS:
                logger.info(f"speech {idx}: failed {attempts} times, skipping (use --force to redo)")
                continue
        logger.info(f"speech {idx}: transcribing {ap.name}...")
        t0 = time.monotonic()
        tr = whisper_qc.transcribe_speech(ap, language=whisper_language(s, args.language),
                                          model_size=args.model,
                                          timeout=args.timeout or speech_timeout(ap))
        if tr is None:
            results[idx] = {"speechIndex": idx, "ok": False, "audio_file": ap.name,
                            "attempts": attempts + 1}
            failed += 1
            save()
            continue
        entry = {
            "speechIndex": idx,
            "ok": True,
            "audio_file": ap.name,
            "transcribe_seconds": round(time.monotonic() - t0, 1),
            **tr,
        }
        if args.with_speaker_check:
            logger.info(f"speech {idx}: speaker-change pass...")
            sp = whisper_qc.detect_speaker_changes(ap)
            if sp is not None:
                entry["speaker_check"] = sp
        results[idx] = entry
        transcribed += 1
        save()

    logger.info(f"wrote {out_path}: {transcribed} transcribed, {failed} failed")
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
        for c in spoken_text_contents(speech):
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

    # Text-inflation: the proceedings text is far larger than the Whisper
    # transcript of the same clip (the merger over-attached neighbouring turns).
    # The DE rubric only caught this via n_speech_ids_attached, which AT never
    # sets — so trigger on the chars ratio directly. When there is no Whisper
    # transcript to compare against, fall back to an extreme proceedings cps.
    # Gated on a substantial proceedings text so a high ratio on a tiny chair
    # micro-utterance (already caught by the sim/boundary signals) doesn't read
    # as inflation — this targets a real block of neighbouring turns.
    inflation = (chars_p / chars_w) if chars_w > 0 else None
    if chars_p > 800:
        if inflation is not None and inflation >= 3.0:
            suspect_score += min(math.log(inflation) * 3.0, 8.0)
        elif inflation is None and cps_p > 50:
            suspect_score += min(math.log(max(cps_p, 0.01) / 17.0) * 3.0, 8.0)

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
        "text_inflation_ratio": round(inflation, 1) if inflation is not None else None,
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
        out.append(f"- chars: proceedings={r['chars_proceedings']}, whisper={r['chars_whisper']} "
                   f"(inflation ×{r['text_inflation_ratio']})")
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


def write_session_qc(data_dir: Path, session: str) -> list[dict] | None:
    """Compare a session's text with its transcript and write both outputs: the
    Markdown report and ``<session>-qc.json``, the per-speech metrics with the
    signature of the text and the timestamp of the transcript they describe.

    Returns the metrics, or ``None`` when the session has no transcript.
    """
    from optv.shared.publish import text_signature

    session_doc = load_session(data_dir, session)
    wp = whisper_path(data_dir, session)
    if not wp.exists():
        return None
    whisper_doc = json.load(open(wp))
    transcripts = whisper_doc.get("speeches", [])
    speeches = session_doc.get("data", [])
    whisper_by_idx, outdated = current_transcripts(speeches, whisper_doc)
    if outdated:
        logger.warning(f"{session}: {outdated} transcripts no longer match their speech and are left out")

    speeches_by_idx = {s["speechIndex"]: s for s in speeches}

    rows = [compute_metrics(s, whisper_by_idx.get(s["speechIndex"])) for s in speeches]

    md = render_markdown(session_doc, whisper_doc, rows, speeches_by_idx, whisper_by_idx)
    rp = report_path(data_dir, session)
    rp.parent.mkdir(parents=True, exist_ok=True)
    with open(rp, "w") as f:
        f.write(md)
    logger.info(f"wrote {rp}")

    _write_json(qc_path(data_dir, session), {
        "session": session,
        "text_signature": text_signature(speeches),
        "whisper_generated_at": whisper_doc.get("generated_at"),
        "model": whisper_doc.get("model"),
        "transcribed": len(whisper_by_idx),
        "outdated": outdated,
        "failed": sum(1 for s in transcripts if not s.get("ok")),
        "rows": rows,
    })
    logger.info(f"wrote {qc_path(data_dir, session)}")
    return rows


def cmd_diff(args) -> int:
    data_dir = Path(args.data_dir).resolve()
    rows = write_session_qc(data_dir, args.session)
    if rows is None:
        logger.error(f"no whisper file: {whisper_path(data_dir, args.session)}. Run `transcribe` first.")
        return 2
    if args.quiet:
        return 0

    print(render_terminal(rows))
    print()
    print("== anomalies grouped by agendaItem.type ==")
    print(render_type_breakdown(rows))
    print()
    suspects = sorted(rows, key=lambda r: r["suspect_score"], reverse=True)[:5]
    print(f"Top suspects: {[r['speechIndex'] for r in suspects]}")
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
    p_tr.add_argument("--language", default=None,
                      help="Whisper language for every speech; default: each speech's originalLanguage")
    p_tr.add_argument("--model", default=whisper_qc.DEFAULT_MODEL)
    p_tr.add_argument("--timeout", type=int, default=None,
                      help="per-speech wall-clock timeout in seconds; default: three times the "
                           f"clip length, at least {MIN_TIMEOUT} and at most {whisper_qc.DEFAULT_TIMEOUT}")
    p_tr.add_argument("--with-speaker-check", action="store_true",
                      help="also run Resemblyzer speaker change detection")
    p_tr.add_argument("--force", action="store_true",
                      help="redo speeches that are already in the cache")
    p_tr.set_defaults(func=cmd_transcribe)

    p_diff = sub.add_parser("diff", help="render diff report from cached whisper output")
    p_diff.add_argument("--data-dir", required=True)
    p_diff.add_argument("--session", required=True)
    p_diff.add_argument("--quiet", action="store_true",
                        help="write the report and the metrics file without the terminal summary")
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
