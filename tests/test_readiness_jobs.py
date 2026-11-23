"""Estimating and running the Whisper jobs of the readiness tooling, without Whisper: the cost model, what a plan may cover, the confirmation token, and the detached runner's bookkeeping."""

import datetime
import fcntl
import json
import signal
import sys
import time

import pytest

from optv.scripts.readiness import estimate, jobs, runner
from optv.scripts.readiness.config import load_declaration
from optv.shared.whisper_diff import qc_path, write_session_qc
from tests.readiness_fixtures import (clean_session, clip, stage_clips, write_session,
                                      write_transcripts)

# The synthetic sessions take the keys of a real parliament's first three declared sample
# sessions, so that the plans are built from its manifest like any other.
CODE = "DE-NW"
S1, S2, S3 = load_declaration(CODE).sample_ids[:3]
SAMPLES = {S1: 115, S2: 116, S3: 117}
OUTSIDE = "00000"


CLIP_SECONDS = {}


@pytest.fixture(autouse=True)
def clip_lengths(monkeypatch):
    """The staged clips are not audio; their length is whatever a test says, by default unknown."""
    CLIP_SECONDS.clear()
    monkeypatch.setattr("optv.shared.whisper_diff.clip_seconds",
                        lambda audio: CLIP_SECONDS.get(audio.name, 0.0))
    return CLIP_SECONDS


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OPTV_DATA_ROOT", str(tmp_path / "repos"))
    monkeypatch.setenv("OPTV_STATE_DIR", str(tmp_path / "state"))
    return tmp_path / "repos" / f"OpenParliamentTV-Data-{CODE}"


@pytest.fixture
def docs(data_dir):
    """Three published sample sessions with staged clips: the first untranscribed, the second transcribed without figures, the third complete."""
    sessions = {session: clean_session(session, number, duration=120.0)
                for session, number in SAMPLES.items()}
    for session, doc in sessions.items():
        write_session(data_dir, session, doc)
        stage_clips(data_dir, doc)
    write_transcripts(data_dir, S2, sessions[S2])
    write_transcripts(data_dir, S3, sessions[S3])
    write_session_qc(data_dir, S3)
    return sessions


def _steps(plan):
    return [(step["kind"], step["session"]) for step in plan["steps"]]


def test_a_speech_costs_at_least_the_model_start():
    assert estimate.speech_cost(4.0) == estimate.MIN_SPEECH_SECONDS
    assert estimate.speech_cost(1000.0) == 790.0
    assert estimate.speech_cost(1000.0, factor=1.2) == 1200.0


def test_the_factor_is_the_default_until_enough_audio_is_on_record(data_dir, docs):
    assert estimate.calibrate(data_dir) == (estimate.REALTIME_FACTOR, "default")
    path = write_transcripts(data_dir, S2, docs[S2])
    cache = json.loads(path.read_text())
    for entry in cache["speeches"]:
        entry["duration"], entry["transcribe_seconds"] = 1800.0, 2160.0
    path.write_text(json.dumps(cache))
    # The second session at 1.2 and the short clips of the third at 0.8 times their length.
    assert estimate.calibrate(data_dir) == (1.175, "recorded")
    assert estimate.backtest(CODE)["speeches"] == 8


def test_a_plan_covers_exactly_the_outstanding_work(data_dir, docs):
    plan = estimate.build_plan([CODE])
    assert _steps(plan) == [("transcribe", S1), ("diff", S1), ("diff", S2)]
    transcribe = plan["steps"][0]
    assert (transcribe["speeches"], transcribe["audio_seconds"]) == (4, 480)
    assert transcribe["expected_seconds"] == round(4 * 120 * estimate.REALTIME_FACTOR)
    assert plan["expected_seconds"] == sum(step["expected_seconds"] for step in plan["steps"])
    assert plan["low_seconds"] < plan["expected_seconds"] < plan["high_seconds"]
    assert plan["skipped"] == []
    text = estimate.render_plan(plan)
    assert f"transcribe {CODE} {S1}" in text and "downloads nothing" in text
    assert text.endswith(f"start {plan['id']} --confirm {estimate.plan_token(plan)}")


def test_a_session_without_staged_audio_is_reported_not_fetched(data_dir, docs):
    for speech in docs[S1]["data"]:
        clip(data_dir, speech).unlink()
    plan = estimate.build_plan([CODE], {CODE: [S1]})
    assert plan["steps"] == []
    assert plan["skipped"] == [{"parliament": CODE, "session": S1,
                                "reason": "audio not staged (4 text speeches without a cached clip)"}]
    assert "Nothing to transcribe" in estimate.render_plan(plan)


def test_a_session_whose_clips_are_outdated_is_reported_not_transcribed(data_dir, docs, clip_lengths):
    for speech in docs[S1]["data"]:
        clip_lengths[clip(data_dir, speech).name] = 900.0
    plan = estimate.build_plan([CODE], {CODE: [S1]})
    assert plan["steps"] == []
    assert plan["skipped"] == [{"parliament": CODE, "session": S1, "reason": (
        "cached audio is outdated (4 of 4 clips differ in length from their speech); "
        "it has to be staged again")}]


def test_missing_audio_is_reported_for_a_partly_transcribed_session_too(data_dir, docs):
    write_transcripts(data_dir, S1, docs[S1], skip=(3, 4))
    for speech in docs[S1]["data"][2:]:
        clip(data_dir, speech).unlink()
    plan = estimate.build_plan([CODE], {CODE: [S1]})
    assert _steps(plan) == [("diff", S1)]
    assert plan["skipped"] == [{"parliament": CODE, "session": S1,
                                "reason": "audio not staged (2 text speeches without a cached clip)"}]


def test_a_speech_that_failed_twice_is_not_planned_again(data_dir, docs):
    path = write_transcripts(data_dir, S1, docs[S1], skip=(3, 4))
    cache = json.loads(path.read_text())
    cache["speeches"] += [{"speechIndex": 3, "ok": False, "attempts": 2},
                          {"speechIndex": 4, "ok": False, "attempts": 1}]
    path.write_text(json.dumps(cache))
    plan = estimate.build_plan([CODE], {CODE: [S1]})
    assert plan["steps"][0]["speeches"] == 1


def test_transcripts_outdated_by_a_re_merge_are_planned_again(data_dir, docs):
    assert estimate.build_plan([CODE], {CODE: [S3]})["steps"] == []
    write_transcripts(data_dir, S3, clean_session(S3, 117, duration=400.0))
    plan = estimate.build_plan([CODE], {CODE: [S3]})
    assert _steps(plan) == [("transcribe", S3), ("diff", S3)]
    assert plan["steps"][0]["speeches"] == 4


def test_only_declared_sample_sessions_can_be_planned(data_dir, docs):
    with pytest.raises(estimate.ScopeError, match=f"{OUTSIDE} is not a declared sample session"):
        estimate.build_plan([CODE], {CODE: [S1, OUTSIDE]})


def test_a_live_parliament_may_go_beyond_its_samples(tmp_path, monkeypatch):
    monkeypatch.setenv("OPTV_DATA_ROOT", str(tmp_path))
    (tmp_path / "OpenParliamentTV-Data-DE").mkdir()
    plan = estimate.build_plan(["DE"], {"DE": ["21001"]})
    assert plan["skipped"] == [{"parliament": "DE", "session": "21001", "reason": "not published"}]


def test_a_parliament_without_data_here_is_reported(tmp_path, monkeypatch):
    monkeypatch.setenv("OPTV_DATA_ROOT", str(tmp_path))
    plan = estimate.build_plan(["FI"])
    assert plan["skipped"] == [{"parliament": "FI", "reason": "no data repository on this machine"}]


def test_the_token_identifies_one_plan(data_dir, docs):
    plan = estimate.build_plan([CODE], now=datetime.datetime(2026, 1, 1, 12))
    token = estimate.plan_token(plan)
    assert estimate.plan_token(json.loads(json.dumps(plan))) == token
    plan["steps"][0]["session"] = OUTSIDE
    assert estimate.plan_token(plan) != token


def _planned(codes=(CODE,), **kwargs):
    plan = estimate.build_plan(list(codes), **kwargs)
    jobs.save_plan(plan)
    return plan


def test_start_needs_the_token_of_the_plan(data_dir, docs):
    plan = _planned()
    with pytest.raises(jobs.JobError, match="token does not belong"):
        jobs.start(plan["id"], "000000000000")
    with pytest.raises(jobs.JobError, match="no job"):
        jobs.start("20260101-000000-XX", "000000000000")
    assert jobs.list_jobs()[0]["status"] == "planned"


def test_start_refuses_an_old_estimate(data_dir, docs):
    plan = _planned(now=datetime.datetime.now() - datetime.timedelta(hours=25))
    with pytest.raises(jobs.JobError, match="older than 24 h"):
        jobs.start(plan["id"], estimate.plan_token(plan))


def test_start_refuses_when_the_data_changed_since_the_estimate(data_dir, docs):
    plan = _planned()
    write_transcripts(data_dir, S1, docs[S1])
    with pytest.raises(jobs.JobError, match="data changed since the estimate"):
        jobs.start(plan["id"], estimate.plan_token(plan))


def test_start_refuses_while_another_job_runs(data_dir, docs, tmp_path):
    plan = _planned()
    runner.lock_path().parent.mkdir(parents=True, exist_ok=True)
    with open(runner.lock_path(), "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        lock.write("20260101-000000-OTHER")
        lock.flush()
        assert jobs.running_job() == "20260101-000000-OTHER"
        with pytest.raises(jobs.JobError, match="20260101-000000-OTHER is running"):
            jobs.start(plan["id"], estimate.plan_token(plan))
        assert runner.Runner(plan["id"]).run() == runner.EXIT_LOCKED
    assert jobs.running_job() is None


def test_a_transcription_job_needs_whisper_on_this_machine(data_dir, docs, monkeypatch, tmp_path):
    plan = _planned()
    monkeypatch.setenv("HF_HUB_CACHE", str(tmp_path / "empty-hub"))
    with pytest.raises(jobs.JobError, match="Whisper model is not in"):
        jobs.preflight(plan)


# ---- the runner, with the two real commands replaced by trivial ones ----

@pytest.fixture
def scripted(data_dir, docs, monkeypatch, tmp_path):
    """Run plans whose steps touch a marker file instead of transcribing. ``behaviour`` maps a session to ``ok``, ``exit-1`` (fails) or ``no-result`` (exits cleanly without a result)."""
    behaviour = {}
    markers = tmp_path / "markers"
    markers.mkdir()
    runs = tmp_path / "runs.log"

    def marker(step):
        return markers / f"{step['kind']}-{step['session']}"

    def command(plan, step):
        mode = behaviour.get(step["session"], "ok")
        script = f"open({str(runs)!r}, 'a').write({step['kind'] + ' ' + step['session']!r} + '\\n')\n"
        if mode == "ok":
            script += f"open({str(marker(step))!r}, 'w').close()\n"
        elif mode == "exit-1":
            script += "raise SystemExit(1)\n"
        return [sys.executable, "-c", script]

    monkeypatch.setattr(runner, "step_command", command)
    monkeypatch.setattr(runner, "postcondition",
                        lambda step: (marker(step).exists(), "marker" if marker(step).exists() else "no marker"))
    monkeypatch.setattr(runner.shutil, "which", lambda name: None)
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}

    class Scripted:
        def run(self, job_id):
            assert runner.Runner(job_id).run() == 0
            return json.loads((runner.job_dir(job_id) / "state.json").read_text())

        def executed(self):
            return runs.read_text().splitlines() if runs.exists() else []

    scripted = Scripted()
    scripted.behaviour = behaviour
    yield scripted
    for sig, handler in handlers.items():
        signal.signal(sig, handler)


def test_a_job_runs_its_steps_in_order(scripted):
    plan = _planned()
    state = scripted.run(plan["id"])
    assert state["status"] == "done"
    assert [step["status"] for step in state["steps"]] == ["done", "done", "done"]
    assert scripted.executed() == [f"transcribe {S1}", f"diff {S1}", f"diff {S2}"]
    assert "pid" not in state and state["runs"] == 1
    assert jobs.list_jobs()[0]["steps_done"] == 3
    assert jobs.describe(plan["id"])["status"] == "done"


def test_one_failing_session_does_not_stop_the_others(scripted):
    scripted.behaviour[S1] = "exit-1"
    plan = _planned()
    state = scripted.run(plan["id"])
    assert state["status"] == "done_with_failures"
    assert [(step["status"], step.get("exit_code")) for step in state["steps"]] == [
        ("failed", 1), ("failed", 1), ("done", 0)]


def test_a_clean_exit_without_a_result_is_a_failure(scripted):
    scripted.behaviour.update({S1: "no-result", S2: "no-result"})
    plan = _planned()
    state = scripted.run(plan["id"])
    assert state["status"] == "failed"
    assert [(step["status"], step["exit_code"], step["detail"]) for step in state["steps"]] == [
        ("failed", 0, "no marker")] * 3


def test_resume_repeats_only_what_is_unfinished(scripted):
    scripted.behaviour[S1] = "exit-1"
    plan = _planned()
    scripted.run(plan["id"])
    scripted.behaviour.clear()
    state = scripted.run(plan["id"])
    assert state["status"] == "done" and state["runs"] == 2
    assert scripted.executed() == [f"transcribe {S1}", f"diff {S1}", f"diff {S2}",
                                   f"transcribe {S1}", f"diff {S1}"]


def test_a_job_whose_runner_vanished_shows_as_interrupted(scripted):
    plan = _planned()
    runner.write_json(runner.job_dir(plan["id"]) / "state.json", {
        "id": plan["id"], "status": "running", "pid": 999999, "runs": 1,
        "steps": [{"name": f"transcribe {CODE} {S1}", "status": "running", "pid": 999998}]})
    assert jobs.list_jobs()[0]["status"] == "interrupted"
    assert jobs.describe(plan["id"])["status"] == "interrupted"
    assert jobs.cancel(plan["id"]) == f"job {plan['id']} is not running"
    with pytest.raises(jobs.JobError, match="was started before"):
        jobs.start(plan["id"], estimate.plan_token(plan))


def test_the_runner_knows_only_its_two_commands():
    plan = {"python": "/venv/bin/python"}
    step = {"kind": "transcribe", "parliament": "NO", "session": "22_11517", "data_dir": "/data/NO"}
    assert runner.step_command(plan, step) == [
        "/venv/bin/python", "-m", "optv.shared.whisper_diff", "transcribe",
        "--data-dir", "/data/NO", "--session", "22_11517"]
    assert runner.step_command(plan, dict(step, language="no"))[-2:] == ["--language", "no"]
    assert runner.step_command(plan, dict(step, kind="diff"))[-1] == "--quiet"
    with pytest.raises(ValueError):
        runner.step_command(plan, dict(step, kind="download"))


def test_a_detached_job_runs_to_the_end_on_its_own(data_dir, docs):
    """The real path end to end, on a plan that needs no Whisper: only the figures of the second session are missing."""
    plan = _planned(sessions={CODE: [S2]})
    assert _steps(plan) == [("diff", S2)]
    pid = jobs.start(plan["id"], estimate.plan_token(plan))
    assert pid > 0
    deadline = time.monotonic() + 60
    while jobs.describe(plan["id"])["status"] in ("running", "planned") and time.monotonic() < deadline:
        time.sleep(0.2)
    job = jobs.describe(plan["id"])
    assert job["status"] == "done", jobs.tail_logs(plan["id"])
    assert job["steps"][0]["detail"] == "QC figures written"
    assert qc_path(data_dir, S2).is_file()
    assert jobs.running_job() is None
    assert "job " + plan["id"] in jobs.tail_logs(plan["id"])

    with pytest.raises(jobs.JobError, match="was started before"):
        jobs.start(plan["id"], estimate.plan_token(plan))
    with pytest.raises(jobs.JobError, match="is done"):
        jobs.resume(plan["id"])
    assert estimate.build_plan([CODE], {CODE: [S2]})["steps"] == []


def test_estimates_never_started_are_dropped_after_a_day(data_dir, docs):
    old = _planned(now=datetime.datetime.now() - datetime.timedelta(hours=30))
    started = _planned(now=datetime.datetime.now() - datetime.timedelta(hours=40))
    runner.write_json(runner.job_dir(started["id"]) / "state.json", {"id": started["id"], "status": "done"})
    fresh = _planned()
    assert [job["id"] for job in jobs.list_jobs()] == sorted([started["id"], fresh["id"]])
    assert not runner.job_dir(old["id"]).exists()
