"""The detached process that works through a job's plan: ``python -m optv.scripts.readiness.runner JOB_ID``.

Started by ``jobs.start`` or ``jobs.resume``, never by hand. It holds the machine-wide runner lock for as long as it lives, runs the plan's steps one after the other, and records each step's outcome in the job's ``state.json``. A step counts as done when its result is on disk, not when its process exited cleanly.
"""

from __future__ import annotations

import argparse
import datetime
import fcntl
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from . import config as cfg

logger = logging.getLogger("optv.scripts.readiness.runner")

DONE = "done"
FAILED = "failed"
RUNNING = "running"
PENDING = "pending"

EXIT_LOCKED = 3
# `whisper_diff transcribe` refusing clips that were cut before a re-merge.
EXIT_OUTDATED_CLIPS = 3
# Seconds a step's processes get to stop after a cancel before they are killed.
CANCEL_GRACE = 15


def job_dir(job_id: str) -> Path:
    return cfg.state_dir() / "jobs" / job_id


def lock_path() -> Path:
    return cfg.state_dir() / "runner.lock"


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def write_json(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    partial.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
    os.replace(partial, path)


def step_name(step: dict) -> str:
    return f"{step['kind']} {step['parliament']} {step['session']}"


def step_command(plan: dict, step: dict) -> list[str]:
    """The command of a step. A plan names what to do, never how: the runner knows two commands."""
    base = [plan["python"], "-m", "optv.shared.whisper_diff", step["kind"],
            "--data-dir", step["data_dir"], "--session", step["session"]]
    if step["kind"] == "transcribe":
        return base + (["--language", step["language"]] if step.get("language") else [])
    if step["kind"] == "diff":
        return base + ["--quiet"]
    raise ValueError(f"unknown step kind {step['kind']!r}")


def transcription_progress(step: dict) -> tuple[int, int]:
    """Clips of a session with a transcript, and clips in all."""
    from optv.shared.whisper_diff import audio_path_for, current_transcripts, whisper_path
    data_dir = Path(step["data_dir"])
    doc = read_json(data_dir / "processed" / f"{step['session']}-session.json") or {}
    whisper = read_json(whisper_path(data_dir, step["session"])) or {}
    transcribed, _ = current_transcripts(doc.get("data") or [], whisper)
    clips = [speech.get("speechIndex") for speech in doc.get("data") or []
             if audio_path_for(data_dir, speech["electoralPeriod"]["number"],
                               speech["session"]["number"], speech["speechIndex"]).exists()]
    return sum(1 for index in clips if index in transcribed), len(clips)


def postcondition(step: dict) -> tuple[bool, str]:
    """Whether a step's result is on disk, and a line saying what is there."""
    if step["kind"] == "transcribe":
        done, clips = transcription_progress(step)
        return clips > 0 and done == clips, f"{done} of {clips} clips transcribed"
    from optv.shared.publish import text_signature
    from optv.shared.whisper_diff import qc_path, whisper_path
    data_dir = Path(step["data_dir"])
    figures = read_json(qc_path(data_dir, step["session"]))
    whisper = read_json(whisper_path(data_dir, step["session"]))
    doc = read_json(data_dir / "processed" / f"{step['session']}-session.json")
    if figures is None or whisper is None or doc is None:
        return False, "no QC figures"
    current = (figures.get("text_signature") == text_signature(doc.get("data") or [])
               and figures.get("whisper_generated_at") == whisper.get("generated_at"))
    return current, "QC figures written" if current else "QC figures outdated"


def final_status(steps: list[dict], cancelled: bool) -> str:
    if cancelled:
        return "cancelled"
    done = sum(1 for step in steps if step["status"] == DONE)
    if done == len(steps):
        return "done"
    return "done_with_failures" if done else "failed"


def _now() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


class Runner:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.dir = job_dir(job_id)
        self.plan = read_json(self.dir / "plan.json")
        self.state = read_json(self.dir / "state.json") or {}
        self.cancelled = False
        self.cancelled_at = 0.0
        self.child: subprocess.Popen | None = None

    def save(self) -> None:
        write_json(self.dir / "state.json", self.state)

    def on_cancel(self, signum, frame) -> None:
        self.cancelled = True
        self.cancelled_at = time.monotonic()
        if self.child is not None and self.child.poll() is None:
            try:
                os.killpg(self.child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def run_step(self, index: int, step: dict, record: dict) -> None:
        log_path = self.dir / "steps" / f"{index + 1:02d}-{step['kind']}-{step['parliament']}-{step['session']}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        if step["kind"] == "transcribe":
            # The model is in the local cache (checked before the job started); never fetch it.
            env["HF_HUB_OFFLINE"] = "1"
        started = time.monotonic()
        with open(log_path, "a") as log:
            log.write(f"--- {_now()} {step_name(step)}\n")
            log.flush()
            # Its own process group, so a cancel reaches the transcription worker it spawns.
            self.child = subprocess.Popen(step_command(self.plan, step), cwd=self.plan["tools"],
                                          stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                          env=env, start_new_session=True)
            record.update(status=RUNNING, started=_now(), pid=self.child.pid, log=str(log_path))
            record.pop("finished", None)
            self.save()
            while True:
                try:
                    exit_code = self.child.wait(timeout=5)
                    break
                except subprocess.TimeoutExpired:
                    if self.cancelled and time.monotonic() - self.cancelled_at > CANCEL_GRACE:
                        try:
                            os.killpg(self.child.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
        self.child = None
        satisfied, detail = postcondition(step)
        if not satisfied and step["kind"] == "transcribe" and exit_code == EXIT_OUTDATED_CLIPS:
            detail = "the cached audio no longer matches the speeches; it has to be staged again"
        record.update(status=DONE if satisfied else FAILED, finished=_now(), exit_code=exit_code,
                      seconds=round(time.monotonic() - started), detail=detail)
        record.pop("pid", None)
        self.save()

    def run(self) -> int:
        if self.plan is None:
            logger.error(f"no plan for job {self.job_id}")
            return 2
        cfg.state_dir().mkdir(parents=True, exist_ok=True)
        lock = open(lock_path(), "a+")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logger.error("another readiness job holds the runner lock")
            return EXIT_LOCKED
        lock.seek(0)
        lock.truncate()
        lock.write(self.job_id)
        lock.flush()

        signal.signal(signal.SIGTERM, self.on_cancel)
        signal.signal(signal.SIGINT, self.on_cancel)
        if sys.platform == "darwin" and shutil.which("caffeinate"):
            # Keep the machine from idle-sleeping for as long as this process lives.
            subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())], stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        steps = self.plan["steps"]
        records = self.state.get("steps") or []
        if len(records) != len(steps):
            records = [{"name": step_name(step), "status": PENDING} for step in steps]
        self.state.update(id=self.job_id, status=RUNNING, pid=os.getpid(), steps=records)
        self.state.setdefault("started", _now())
        self.state["runs"] = self.state.get("runs", 0) + 1
        self.state.pop("finished", None)
        self.save()
        logger.info(f"job {self.job_id}: {len(steps)} steps")

        for index, (step, record) in enumerate(zip(steps, records)):
            if self.cancelled:
                break
            satisfied, detail = postcondition(step)
            if satisfied:
                if record["status"] != DONE:
                    record.update(status=DONE, detail=f"{detail}; nothing to do")
                    self.save()
                continue
            logger.info(f"step {index + 1}/{len(steps)}: {record['name']}")
            self.run_step(index, step, record)
            logger.info(f"step {index + 1}/{len(steps)}: {record['status']} ({record.get('detail')})")

        self.state.update(status=final_status(records, self.cancelled), finished=_now())
        self.state.pop("pid", None)
        self.save()
        logger.info(f"job {self.job_id}: {self.state['status']}")
        return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m optv.scripts.readiness.runner",
        description="Work through the plan of a readiness job. Started detached by "
                    "`python -m optv.scripts.readiness start`; not meant to be run by hand.")
    parser.add_argument("job_id", metavar="JOB_ID")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    return Runner(args.job_id).run()


if __name__ == "__main__":
    sys.exit(main())
