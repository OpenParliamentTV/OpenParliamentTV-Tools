"""Plan the outstanding Whisper work of the sample sessions and estimate how long it takes.

A plan lists one ``transcribe`` and one ``diff`` step per session that still has clips without a transcript. It only ever covers audio the align stage already cached: nothing is downloaded. The plan is the unit a person confirms; ``jobs.start`` runs exactly the steps it lists and only when handed the token of the plan as estimated.

A sample-run plan is the other kind: it runs a parliament's own workflow for its sample sessions, with the arguments its manifest declares for that (``readiness.sample_run``). That is the one thing here that downloads.
"""

from __future__ import annotations

import datetime
import json
import sys
from hashlib import blake2b
from pathlib import Path

from . import config as cfg
from .criteria import SampleData, missing_stages
from .scorecard import _load_json, _session_qc, build_context
from .speeches import clip_path, has_text, speech_duration

PLAN_SCHEMA = 1
# Transcription seconds per second of audio (faster-whisper large-v3, CPU, int8), and
# what a speech costs at least: the model is loaded anew for each one.
REALTIME_FACTOR = 0.79
MIN_SPEECH_SECONDS = 13
# Comparing text and transcript, per speech.
DIFF_SECONDS_PER_SPEECH = 0.5
# The estimate is shown with this range around it.
RANGE = (0.85, 1.3)
# Audio a parliament needs on record before its own figures replace the default factor.
CALIBRATION_MIN_AUDIO_SECONDS = 3600
CALIBRATION_BOUNDS = (0.3, 2.0)
PLAN_MAX_AGE_HOURS = 24
# The stages of a sample run and the workflow flag of each.
STAGE_FLAGS = (("download", "--download-original"), ("merge", "--merge-speeches"),
               ("nel", "--link-entities"), ("align", "--align-sentences"),
               ("ner", "--extract-entities"))


class ScopeError(ValueError):
    """A plan was asked to cover a session it must not cover."""


def speech_cost(duration: float, factor: float = REALTIME_FACTOR) -> float:
    return max(MIN_SPEECH_SECONDS, factor * duration)


def _recorded(data_dir: Path) -> list[tuple[str, float, float]]:
    """(session, audio seconds, transcription seconds) of every transcribed speech on record."""
    records = []
    for path in sorted((data_dir / "cache" / "whisper").glob("*-whisper.json")):
        doc = _load_json(path) or {}
        session = path.name.removesuffix("-whisper.json")
        for entry in doc.get("speeches") or []:
            duration, seconds = entry.get("duration"), entry.get("transcribe_seconds")
            if entry.get("ok") and duration and seconds:
                records.append((session, float(duration), float(seconds)))
    return records


def calibrate(data_dir: Path) -> tuple[float, str]:
    """The realtime factor to estimate with and what it rests on: this parliament's recorded transcriptions when there are enough, else the default."""
    records = [r for r in _recorded(data_dir) if r[2] > MIN_SPEECH_SECONDS]
    audio = sum(r[1] for r in records)
    if audio < CALIBRATION_MIN_AUDIO_SECONDS:
        return REALTIME_FACTOR, "default"
    factor = sum(r[2] for r in records) / audio
    low, high = CALIBRATION_BOUNDS
    return round(min(max(factor, low), high), 3), "recorded"


def backtest(code: str, data_dir: Path | None = None) -> dict:
    """Recorded transcription time of a parliament against what the default cost model predicts for the same audio."""
    records = _recorded(data_dir or cfg.data_dir(code))
    actual = sum(r[2] for r in records)
    predicted = sum(speech_cost(r[1]) for r in records)
    return {"speeches": len(records), "audio_seconds": round(sum(r[1] for r in records)),
            "actual_seconds": round(actual), "predicted_seconds": round(predicted),
            "ratio": round(predicted / actual, 3) if actual else None}


def render_backtest(code: str, result: dict) -> str:
    if not result["speeches"]:
        return f"{code}: no recorded transcriptions"
    return (f"{code}: {result['speeches']} speeches, {_hours(result['audio_seconds'])} of audio, "
            f"took {_hours(result['actual_seconds'])}, the model predicts "
            f"{_hours(result['predicted_seconds'])} (x{result['ratio']})")


def _pending(data_dir: Path, sample: SampleData) -> tuple[list[dict], int, int]:
    """Speeches a transcribe run would work on, how many text speeches have no cached clip, and how many transcripts exist."""
    from optv.shared.whisper_diff import MAX_ATTEMPTS, current_transcripts, whisper_path
    whisper = _load_json(whisper_path(data_dir, sample.sample.id)) or {}
    # A transcript a re-merge left at the wrong index is as good as none.
    current, _ = current_transcripts(sample.speeches, whisper)
    failed = {entry.get("speechIndex"): entry for entry in whisper.get("speeches") or []
              if not entry.get("ok")}
    pending, without_clip = [], 0
    for speech in sample.speeches:
        index = speech.get("speechIndex")
        if index in current:
            continue
        if not clip_path(data_dir, speech).exists():
            without_clip += 1 if has_text(speech) else 0
            continue
        if index in failed and failed[index].get("attempts", 1) >= MAX_ATTEMPTS:
            continue
        pending.append(speech)
    return pending, without_clip, len(current)


def _session_steps(code: str, data_dir: Path, sample: SampleData, factor: float,
                   language: str | None) -> tuple[list[dict], dict | None]:
    """The steps one session needs, or why it gets none."""
    sid = sample.sample.id
    if sample.doc is None:
        return [], {"parliament": code, "session": sid, "reason": "not published"}
    if not sample.sample.expect_text:
        return [], None
    pending, without_clip, transcribed = _pending(data_dir, sample)
    diff = {"kind": "diff", "parliament": code, "session": sid, "data_dir": str(data_dir),
            "speeches": len(sample.speeches),
            "expected_seconds": round(len(sample.speeches) * DIFF_SECONDS_PER_SPEECH)}
    if not pending:
        # Nothing to transcribe; say so when that is for want of audio, not for being done.
        reason = None
        if without_clip:
            reason = {"parliament": code, "session": sid,
                      "reason": f"audio not staged ({without_clip} text speeches without a cached clip)"}
        qc = _session_qc(data_dir, sample)
        if transcribed and not (qc.rows is not None and qc.current):
            return [diff], reason
        return [], reason
    from optv.shared.whisper_diff import outdated_clips, too_many_differ
    differing, checked = outdated_clips(data_dir, pending)
    if too_many_differ(differing, checked):
        return [], {"parliament": code, "session": sid,
                    "reason": f"cached audio is outdated ({differing} of {checked} clips differ in length "
                              "from their speech); it has to be staged again"}
    audio = expected = 0.0
    guessed = False
    for speech in pending:
        duration, source = speech_duration(speech, clip=clip_path(data_dir, speech))
        guessed = guessed or source in ("text-length", "unknown")
        audio += duration
        expected += speech_cost(duration, factor)
    transcribe = {"kind": "transcribe", "parliament": code, "session": sid, "data_dir": str(data_dir),
                  "speeches": len(pending), "audio_seconds": round(audio),
                  "expected_seconds": round(expected), "factor": factor}
    if language:
        transcribe["language"] = language
    if without_clip:
        transcribe["without_clip"] = without_clip
    if guessed:
        transcribe["duration_guessed"] = True
    return [transcribe, diff], None


def build_plan(codes: list[str], sessions: dict[str, list[str]] | None = None, *,
               now: datetime.datetime | None = None) -> dict:
    """Plan the Whisper work of ``codes``, by default for all their sample sessions.

    ``sessions`` narrows a parliament to some sessions. A session outside the declared sample sessions is refused unless the parliament is live: only a live parliament is processed beyond its samples.
    """
    now = now or datetime.datetime.now()
    steps, skipped, calibration = [], [], {}
    for code in codes:
        ctx = build_context(code)
        wanted = (sessions or {}).get(code)
        outside = [sid for sid in wanted or [] if sid not in ctx.declaration.sample_ids]
        if outside and not ctx.declaration.is_live:
            raise ScopeError(f"{code}: {', '.join(outside)} is not a declared sample session, "
                             f"and {code} is not live")
        if ctx.config is None:
            skipped.append({"parliament": code, "reason": "no data repository on this machine"})
            continue
        data_dir = ctx.config.dir("data")
        factor, basis = calibrate(data_dir)
        calibration[code] = {"factor": factor, "basis": basis}
        samples = [s for s in ctx.samples if not wanted or s.sample.id in wanted]
        for sid in outside:
            extra = SampleData(cfg.Sample(sid))
            path = ctx.config.file(sid, "processed")
            extra.doc = _load_json(path) if path.is_file() else None
            samples.append(extra)
        for sample in samples:
            session_steps, reason = _session_steps(code, data_dir, sample, factor,
                                                   ctx.declaration.whisper_language)
            steps += session_steps
            if reason:
                skipped.append(reason)
    label = "-".join(codes) if len(codes) <= 3 else f"{len(codes)}-parliaments"
    return _plan(now, label, {"kind": "whisper", "codes": list(codes), "sessions": sessions or {}},
                 steps, skipped, calibration=calibration)


def _plan(now: datetime.datetime, label: str, request: dict, steps: list, skipped: list,
          **extra) -> dict:
    expected = sum(step["expected_seconds"] for step in steps)
    plan = {
        "schema": PLAN_SCHEMA,
        "id": f"{now:%Y%m%d-%H%M%S}-{label}",
        "created": now.isoformat(timespec="seconds"),
        "request": request,
        "tools": str(cfg.tools_root()),
        "python": sys.executable,
        "steps": steps,
        "skipped": skipped,
        **extra,
        "expected_seconds": expected,
        "low_seconds": round(expected * RANGE[0]),
        "high_seconds": round(expected * RANGE[1]),
    }
    plan["inputs"] = inputs_fingerprint(plan)
    return plan


def rebuild(plan: dict) -> dict:
    """The plan its request yields now, to tell whether the data moved since the estimate."""
    request = plan["request"]
    if request.get("kind") == "sample-run":
        return build_sample_run_plan(request["codes"][0], ner_endpoint=request.get("ner_endpoint"))
    return build_plan(request["codes"], request.get("sessions") or None)


def recorded_workflow_seconds(code: str) -> tuple[float, str] | None:
    """Seconds per session that finished sample runs on record took, and whose: this parliament's, else any. ``None`` without a record."""
    from .runner import read_json
    own, other = [], []
    directory = cfg.state_dir() / "jobs"
    for path in sorted(directory.iterdir()) if directory.is_dir() else []:
        plan, state = read_json(path / "plan.json"), read_json(path / "state.json")
        if not plan or not state:
            continue
        for step, record in zip(plan.get("steps") or [], state.get("steps") or []):
            if step.get("kind") != "workflow" or record.get("status") != "done" or not record.get("seconds"):
                continue
            per_session = record["seconds"] / max(len(step.get("sessions") or []), 1)
            (own if step.get("parliament") == code else other).append(per_session)
    for values, basis in ((own, f"earlier sample runs of {code}"), (other, "sample runs of other parliaments")):
        if values:
            return sorted(values)[len(values) // 2], basis
    return None


def build_sample_run_plan(code: str, *, ner_endpoint: str | None = None,
                          now: datetime.datetime | None = None) -> dict:
    """Plan the workflow runs that take a parliament's sample sessions through its stages.

    Each ``readiness.sample_run`` entry of the manifest becomes one step, unless its sessions are complete already. Without such an entry there is no plan: the tooling does not guess how a parliament's downloads are confined.
    """
    now = now or datetime.datetime.now()
    ctx = build_context(code)
    if not ctx.declaration.sample_runs:
        raise ScopeError(f"{code} declares no readiness.sample_run in its manifest; "
                         "the workflow is only run with the arguments declared there")
    if ctx.config is None:
        raise ScopeError(f"no data repository for {code} on this machine")
    data_dir = ctx.config.dir("data")
    supported = ctx.manifest.get("supported_stages") or []
    stages = [stage for stage, _ in STAGE_FLAGS
              if stage in supported and (stage != "ner" or ner_endpoint)]
    skipped = []
    if "ner" in supported and not ner_endpoint:
        skipped.append({"parliament": code, "reason": "NER is left out: no --ner-api-endpoint given"})
    by_id = {s.sample.id: s for s in ctx.samples}
    recorded = recorded_workflow_seconds(code)
    steps = []
    for run in ctx.declaration.sample_runs:
        pending = [sid for sid in run.sessions
                   if by_id[sid].doc is None or missing_stages(by_id[sid], set(stages))]
        if not pending:
            continue
        step = {"kind": "workflow", "parliament": code, "session": ",".join(run.sessions),
                "sessions": list(run.sessions), "pending": pending, "data_dir": str(data_dir),
                "args": list(run.args), "stages": stages, "speeches": len(run.sessions)}
        if "ner" in stages:
            step["ner_endpoint"] = ner_endpoint
        if recorded:
            step["expected_seconds"] = round(recorded[0] * len(pending))
            step["basis"] = recorded[1]
        else:
            step["expected_seconds"] = 0
            step["unestimated"] = True
        steps.append(step)
    return _plan(now, f"{code}-samples",
                 {"kind": "sample-run", "codes": [code], "ner_endpoint": ner_endpoint}, steps, skipped)


def inputs_fingerprint(plan: dict) -> str:
    """Identifies the work a plan covers. Changes when a transcript appeared, a clip was staged or a session was re-merged since the estimate."""
    payload = json.dumps([plan["steps"], plan["skipped"]], sort_keys=True, ensure_ascii=False)
    return blake2b(payload.encode(), digest_size=8).hexdigest()


def plan_token(plan: dict) -> str:
    """What ``start`` must be handed to run this plan: proof that the plan run is the plan estimated."""
    payload = json.dumps(plan, sort_keys=True, ensure_ascii=False)
    return blake2b(payload.encode(), digest_size=6).hexdigest()


def _hours(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f} s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


def render_plan(plan: dict, *, now: datetime.datetime | None = None) -> str:
    steps = plan["steps"]
    sample_run = plan["request"].get("kind") == "sample-run"
    lines = []
    if not steps:
        lines.append("Nothing to run: the sample sessions went through every stage already." if sample_run
                     else "Nothing to transcribe: every cached clip of the requested sessions has a transcript.")
    else:
        lines.append(f"Job {plan['id']}")
        lines.append(f"{'STEP':<34} {'SPEECHES':>8} {'AUDIO':>8} {'EXPECTED':>9}  RANGE")
        for step in steps:
            name = f"{step['kind']} {step['parliament']} {step['session']}"
            if step["kind"] == "transcribe":
                span = (f"{_hours(step['expected_seconds'] * RANGE[0])} to "
                        f"{_hours(step['expected_seconds'] * RANGE[1])}")
                note = " (durations partly guessed)" if step.get("duration_guessed") else ""
                lines.append(f"{name:<34} {step['speeches']:>8} {_hours(step['audio_seconds']):>8} "
                             f"{_hours(step['expected_seconds']):>9}  {span}{note}")
            elif step["kind"] == "workflow":
                expected = "unknown" if step.get("unestimated") else _hours(step["expected_seconds"])
                lines.append(f"{name}")
                lines.append(f"    stages: {', '.join(step['stages'])}; still to do: {', '.join(step['pending'])}; "
                             f"expected: {expected}" + (f" (from {step['basis']})" if step.get("basis") else ""))
                lines.append(f"    runs: python -m optv.parliaments.{step['parliament']}.workflow "
                             f"<data_dir> {' '.join(step['args'])}")
            else:
                lines.append(f"{name:<34} {step['speeches']:>8} {'-':>8} {_hours(step['expected_seconds']):>9}")
        lines.append("")
        unestimated = sum(1 for step in steps if step.get("unestimated"))
        if unestimated == len(steps):
            lines.append("Total: unknown. No sample run is on record to estimate from; "
                         "this run's duration is recorded for the next estimate.")
        else:
            end = (now or datetime.datetime.now()) + datetime.timedelta(seconds=plan["expected_seconds"])
            more = f", plus {unestimated} step(s) without an estimate" if unestimated else ""
            lines.append(f"Total: {_hours(plan['expected_seconds'])} expected "
                         f"({_hours(plan['low_seconds'])} to {_hours(plan['high_seconds'])}){more}; "
                         f"started now it ends around {end:%a %d %b %H:%M}.")
        if sample_run:
            lines.append("The job runs the parliament's workflow for the sessions named and no others. "
                         "It DOWNLOADS from the parliament's servers where the download stage is listed, "
                         "and writes to original/, cache/ and processed/ of the data repository.")
        else:
            lines.append("The job transcribes audio clips that are already cached, downloads nothing, and writes "
                         "only transcripts, reports and QC figures to cache/whisper/ of each data repository.")
        partial = [f"{s['parliament']} {s['session']}: {s['without_clip']}" for s in steps
                   if s.get("without_clip")]
        if partial:
            lines.append(f"Text speeches without a cached clip, left out: {'; '.join(partial)}.")
    for entry in plan["skipped"]:
        session = f" {entry['session']}" if entry.get("session") else ""
        lines.append(f"Not planned: {entry['parliament']}{session}: {entry['reason']}.")
    if steps:
        lines.append("")
        lines.append(f"To start: python -m optv.scripts.readiness start {plan['id']} "
                     f"--confirm {plan_token(plan)}")
    return "\n".join(lines)
