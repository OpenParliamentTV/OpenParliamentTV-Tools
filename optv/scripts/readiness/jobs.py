"""Start, inspect, resume and cancel the detached jobs that run a plan.

A job lives in ``<state_dir>/jobs/<id>/``: ``plan.json`` (written by ``estimate``), ``state.json`` and ``runner.log`` (written by the runner) and one log per step under ``steps/``. One job runs at a time on a machine; the runner holds ``<state_dir>/runner.lock`` while it lives, which is also how a job that died without saying so is told from one that is running.
"""

from __future__ import annotations

import datetime
import fcntl
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

from . import config as cfg
from . import estimate
from .runner import (DONE, RUNNING, job_dir, lock_path, read_json, step_name, transcription_progress,
                     write_json)

WHISPER_MODEL_DIR = "models--Systran--faster-whisper-large-v3"
MIN_FREE_BYTES = 1024 ** 3
# How long `start` waits for the runner to report that it took the lock.
START_WAIT_SECONDS = 15

ACTIVE = ("starting", RUNNING)


class JobError(Exception):
    pass


def prune_expired(now: datetime.datetime | None = None) -> list[str]:
    """Drop the estimates that were never started and can no longer be: every `estimate` leaves a plan behind."""
    directory = cfg.state_dir() / "jobs"
    if not directory.is_dir():
        return []
    limit = (now or datetime.datetime.now()) - datetime.timedelta(hours=estimate.PLAN_MAX_AGE_HOURS)
    removed = []
    for path in sorted(directory.iterdir()):
        plan = read_json(path / "plan.json")
        if plan is None or (path / "state.json").exists():
            continue
        if datetime.datetime.fromisoformat(plan["created"]) < limit:
            shutil.rmtree(path)
            removed.append(plan["id"])
    return removed


def save_plan(plan: dict) -> Path:
    prune_expired()
    path = job_dir(plan["id"]) / "plan.json"
    write_json(path, plan)
    return path


def load_plan(job_id: str) -> dict:
    plan = read_json(job_dir(job_id) / "plan.json")
    if plan is None:
        raise JobError(f"no job {job_id}; `jobs list` shows the known ones")
    return plan


def running_job() -> str | None:
    """The job whose runner holds the lock, or ``None`` when no runner is alive."""
    path = lock_path()
    if not path.is_file():
        return None
    with open(path, "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.seek(0)
            return lock.read().strip() or "unknown"
        fcntl.flock(lock, fcntl.LOCK_UN)
    return None


def status_of(job_id: str, state: dict | None, running: str | None) -> str:
    if not state:
        return "planned"
    if state.get("status") in ACTIVE and running != job_id:
        return "interrupted"
    return state.get("status", "planned")


def _hf_cache() -> Path:
    if os.environ.get("HF_HUB_CACHE"):
        return Path(os.environ["HF_HUB_CACHE"])
    home = os.environ.get("HF_HOME")
    return (Path(home) if home else Path.home() / ".cache" / "huggingface") / "hub"


def _probe_writable(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    probe = directory / f".readiness-probe-{os.getpid()}"
    probe.write_text("")
    probe.unlink()


def preflight(plan: dict) -> None:
    """Refuse a plan whose job could only fail: a transcription that fails for a reason outside the audio still uses up the speech's attempts."""
    if any(step["kind"] == "transcribe" for step in plan["steps"]):
        if importlib.util.find_spec("faster_whisper") is None:
            raise JobError("faster-whisper is not installed; `pip install -r requirements-qc.txt`")
        if not (_hf_cache() / WHISPER_MODEL_DIR).is_dir():
            raise JobError(f"the Whisper model is not in {_hf_cache()}; jobs run offline and do not fetch it")
    for directory in sorted({Path(step["data_dir"]) / "cache" / "whisper" for step in plan["steps"]}
                            | {job_dir(plan["id"])}):
        try:
            _probe_writable(directory)
        except OSError as error:
            raise JobError(f"cannot write to {directory}: {error}") from error
        if shutil.disk_usage(directory).free < MIN_FREE_BYTES:
            raise JobError(f"less than 1 GB free at {directory}")


def _alive(pid) -> bool:
    try:
        os.kill(int(pid), 0)
    except (ProcessLookupError, TypeError, ValueError):
        return False
    except PermissionError:
        return True
    return True


def _leftover_step(state: dict) -> int | None:
    """Pid of a step process of an interrupted run that is still working, if any."""
    for record in state.get("steps") or []:
        pid = record.get("pid")
        if not pid or not _alive(pid):
            continue
        cp = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True)
        if "whisper_diff" in cp.stdout:
            return int(pid)
    return None


def spawn_detached(job_id: str) -> int:
    """Start the runner in its own session, with no tie to this process or its terminal. Returns once the runner reports that it runs."""
    plan = load_plan(job_id)
    directory = job_dir(job_id)
    before = (read_json(directory / "state.json") or {}).get("runs", 0)
    env = dict(os.environ, OPTV_STATE_DIR=str(cfg.state_dir()), PYTHONUNBUFFERED="1")
    with open(directory / "runner.log", "a") as log:
        process = subprocess.Popen([plan["python"], "-m", "optv.scripts.readiness.runner", job_id],
                                   cwd=plan["tools"], stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True, env=env)
    deadline = time.monotonic() + START_WAIT_SECONDS
    while time.monotonic() < deadline:
        state = read_json(directory / "state.json") or {}
        if state.get("runs", 0) > before:
            return process.pid
        if process.poll() is not None:
            break
        time.sleep(0.2)
    if process.poll() is None:
        return process.pid
    raise JobError(f"the runner exited at once (code {process.returncode}):\n{tail_logs(job_id, lines=10)}")


def start(job_id: str, token: str) -> int:
    """Run a plan, given the token ``estimate`` printed for it."""
    plan = load_plan(job_id)
    if token != estimate.plan_token(plan):
        raise JobError("the token does not belong to this plan; start it with the token `estimate` printed")
    if read_json(job_dir(job_id) / "state.json"):
        raise JobError(f"job {job_id} was started before; use `jobs resume {job_id}`")
    if not plan["steps"]:
        raise JobError("the plan has no steps")
    age = datetime.datetime.now() - datetime.datetime.fromisoformat(plan["created"])
    if age > datetime.timedelta(hours=estimate.PLAN_MAX_AGE_HOURS):
        raise JobError(f"the estimate is older than {estimate.PLAN_MAX_AGE_HOURS} h; run `estimate` again")
    request = plan["request"]
    current = estimate.build_plan(request["codes"], request["sessions"] or None)
    if current["inputs"] != plan["inputs"]:
        raise JobError("the data changed since the estimate; run `estimate` again")
    return _launch(job_id, plan)


def resume(job_id: str) -> int:
    """Continue a job that was interrupted, cancelled or left steps unfinished. Finished steps are not repeated."""
    plan = load_plan(job_id)
    state = read_json(job_dir(job_id) / "state.json")
    if not state:
        raise JobError(f"job {job_id} never started; start it with the token `estimate` printed")
    status = status_of(job_id, state, running_job())
    if status in ACTIVE:
        raise JobError(f"job {job_id} is running")
    if status == "done":
        raise JobError(f"job {job_id} is done")
    leftover = _leftover_step(state)
    if leftover:
        raise JobError(f"a step of the interrupted run is still working (pid {leftover}); "
                       "let it finish or stop it, then resume")
    return _launch(job_id, plan)


def _launch(job_id: str, plan: dict) -> int:
    running = running_job()
    if running:
        raise JobError(f"job {running} is running; one job runs at a time")
    preflight(plan)
    return spawn_detached(job_id)


def cancel(job_id: str) -> str:
    load_plan(job_id)
    state = read_json(job_dir(job_id) / "state.json") or {}
    if running_job() != job_id or not state.get("pid"):
        return f"job {job_id} is not running"
    os.kill(int(state["pid"]), signal.SIGTERM)
    return f"job {job_id}: cancel sent; the running step is stopped and the transcripts so far are kept"


def describe(job_id: str) -> dict:
    """A job's plan and state in one view, with the live progress of a running transcription."""
    plan = load_plan(job_id)
    state = read_json(job_dir(job_id) / "state.json") or {}
    records = state.get("steps") or []
    steps = []
    remaining = 0.0
    for index, step in enumerate(plan["steps"]):
        record = records[index] if index < len(records) else {}
        entry = {"name": step_name(step), "status": record.get("status", "pending"),
                 "expected_seconds": step["expected_seconds"]}
        for key in ("seconds", "detail", "started", "finished", "log"):
            if key in record:
                entry[key] = record[key]
        if entry["status"] == RUNNING and step["kind"] == "transcribe":
            done, clips = transcription_progress(step)
            entry["progress"] = f"{done} of {clips} clips"
            # The step covers the clips that were pending when it was planned.
            left = max(clips - done, 0) / step["speeches"] if step["speeches"] else 0
            remaining += step["expected_seconds"] * min(left, 1.0)
        elif entry["status"] != DONE:
            remaining += step["expected_seconds"]
        steps.append(entry)
    status = status_of(job_id, state, running_job())
    job = {"id": job_id, "status": status, "created": plan["created"],
           "expected_seconds": plan["expected_seconds"], "steps": steps,
           "directory": str(job_dir(job_id))}
    for key in ("started", "finished"):
        if key in state:
            job[key] = state[key]
    if status in ACTIVE:
        job["remaining_seconds"] = round(remaining)
    return job


def list_jobs() -> list[dict]:
    directory = cfg.state_dir() / "jobs"
    if not directory.is_dir():
        return []
    running = running_job()
    listing = []
    for path in sorted(directory.iterdir()):
        plan = read_json(path / "plan.json")
        if plan is None:
            continue
        state = read_json(path / "state.json") or {}
        records = state.get("steps") or []
        listing.append({
            "id": plan["id"],
            "status": status_of(plan["id"], state, running),
            "steps": len(plan["steps"]),
            "steps_done": sum(1 for record in records if record.get("status") == DONE),
            "expected_seconds": plan["expected_seconds"],
            "created": plan["created"],
            "started": state.get("started"),
            "finished": state.get("finished"),
        })
    return listing


def render_list(listing: list[dict]) -> str:
    if not listing:
        return "no jobs"
    lines = [f"{'JOB':<40} {'STATUS':<19} {'STEPS':>7} {'EXPECTED':>9}  STARTED"]
    for job in listing:
        lines.append(f"{job['id']:<40} {job['status']:<19} "
                     f"{job['steps_done']}/{job['steps']:<5} {estimate._hours(job['expected_seconds']):>9}  "
                     f"{job.get('started') or '-'}")
    return "\n".join(lines)


def render_job(job: dict) -> str:
    lines = [f"Job {job['id']}: {job['status']}"]
    if job.get("started"):
        lines.append(f"started {job['started']}" + (f", finished {job['finished']}" if job.get("finished") else ""))
    if "remaining_seconds" in job:
        end = datetime.datetime.now() + datetime.timedelta(seconds=job["remaining_seconds"])
        lines.append(f"about {estimate._hours(job['remaining_seconds'])} left, ending around {end:%a %d %b %H:%M}")
    for step in job["steps"]:
        took = f", took {estimate._hours(step['seconds'])}" if "seconds" in step else ""
        note = step.get("progress") or step.get("detail") or ""
        lines.append(f"  {step['status']:<8} {step['name']:<34} expected {estimate._hours(step['expected_seconds'])}"
                     f"{took}{'  ' + note if note else ''}")
    lines.append(f"files: {job['directory']}")
    return "\n".join(lines)


def tail_logs(job_id: str, lines: int = 40) -> str:
    """The end of the runner log and of the newest step log."""
    directory = job_dir(job_id)
    paths = [directory / "runner.log"]
    step_logs = sorted((directory / "steps").glob("*.log"), key=lambda path: path.stat().st_mtime) \
        if (directory / "steps").is_dir() else []
    if step_logs:
        paths.append(step_logs[-1])
    out = []
    for path in paths:
        if not path.is_file():
            continue
        out.append(f"== {path}")
        out += path.read_text(errors="replace").splitlines()[-lines:]
    return "\n".join(out) if out else "no logs yet"
