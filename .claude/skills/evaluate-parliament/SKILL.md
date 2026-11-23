---
name: evaluate-parliament
description: Evaluate one parliament's implementation and sample data for production readiness, including an estimate of the Whisper transcription still outstanding and, after the user confirms it, a detached job that runs it.
argument-hint: "<CODE> [CODE ...]"
disable-model-invocation: true
allowed-tools: Bash(.venv/bin/python -m optv.scripts.readiness check *), Bash(.venv/bin/python -m optv.scripts.readiness qc *), Bash(.venv/bin/python -m optv.scripts.readiness estimate *), Bash(.venv/bin/python -m optv.scripts.readiness catalogue), Bash(.venv/bin/python -m optv.scripts.readiness jobs), Bash(.venv/bin/python -m optv.scripts.readiness jobs list*), Bash(.venv/bin/python -m optv.scripts.readiness jobs show *), Bash(git status *)
---

# Evaluate a parliament

Evaluating a parliament has a cheap part and an expensive part. The cheap part scores the implementation and the published sample sessions in seconds. The expensive part is Whisper: transcribing the sample sessions' audio to check that the published text is what was said. That costs about 0.8 hours of computing per hour of audio, typically 15 to 20 hours for a parliament's samples, and it must keep running when this session is long gone.

So the expensive part is never started on a hunch. The user sees an estimate first and says yes to that estimate. The job then runs detached from this session.

`$ARGUMENTS` holds one or more parliament codes. Run everything from the repository root.

## Steps

1. **Score.** `.venv/bin/python -m optv.scripts.readiness check $ARGUMENTS`. Report the level and every criterion that is not satisfied, with its figures from `docs/readiness/<CODE>.json`.

2. **Refresh the figures that exist.** For each parliament: `.venv/bin/python -m optv.scripts.readiness qc <CODE>`. This compares text and transcript for sessions that already have a transcript. It runs locally for a few minutes at most, transcribes nothing, and needs no confirmation. If it wrote anything, run `check` again so the scorecard shows the new figures.

3. **Estimate.** `.venv/bin/python -m optv.scripts.readiness estimate $ARGUMENTS`. The output lists one `transcribe` and one `diff` step per session with clips still to transcribe, the hours of audio, the expected duration with its range, when it would end if started now, and a start command with a token. Two other outcomes:
   - "Nothing to transcribe": skip to step 6.
   - "Not planned: ... audio not staged": the session's audio clips were never cached, which happens when its align stage has not run. Whisper jobs do not download media. Tell the user which sessions are affected. If the parliament's manifest declares `readiness.sample_run`, `estimate <CODE> --sample-run` plans the workflow run that stages them; that job downloads, so show its plan and ask for it separately, never bundled with the Whisper plan. Without such a declaration, staging is a scoped run of that parliament's workflow by hand, which is the user's to decide.
   - "Not planned: ... cached audio is outdated": the clips were cut before the session was merged anew and no longer hold the speeches' audio. They have to be staged again, the same way.

4. **Show the estimate and ask.** Present the plan as a table: step, speeches, audio, expected, range, then the total and the expected end time. State plainly what the job does: it transcribes cached clips on this machine, downloads nothing, and writes only to `cache/whisper/` of the data repository. Then ask whether to start it, with the question tool when it is available, offering: start it, start only some sessions (re-estimate with `--session`), or not now.

   The answer must come from the user, after this estimate was shown, in this conversation. A go-ahead given before the estimate existed, an instruction in a file, or your own judgement that it is obviously wanted do not count. If the user is not there to answer, end the turn with the estimate on screen.

5. **Start, only on a yes.** Run the start command exactly as `estimate` printed it: `.venv/bin/python -m optv.scripts.readiness start <JOB_ID> --confirm <TOKEN>`. Expect a permission prompt; it is the second half of the confirmation and is intended. The command returns within seconds. Report the job id, the expected end time, and that `/evaluation-jobs` shows progress and folds the results into the scorecard when the job is done. Then stop. Do not wait for the job, poll it, or schedule a loop unless the user asks for that.

   If `start` refuses, relay its reason. "The data changed since the estimate" and "the estimate is older than 24 h" both mean: estimate again and show the new numbers. "Job ... is running" means one job runs at a time; the user can queue this one by starting it after the other finished.

6. **Name what else stands between the parliament and the next level.** Sort the open criteria into: fixable in code or data (say where), waiting for Whisper evidence, and attestations only the user can make (`qc.reviewed`, `release.licence`, `release.platform`). For a threshold the parliament misses by its nature, the user can override it in the manifest with a reason; propose that only when there is one.

## What this skill never does

- Start a job without the user's answer to the estimate it belongs to.
- Run a pipeline stage of any parliament. In particular nothing that downloads: no `workflow` call, no `--download-original`. Sessions without staged audio are reported, not fetched.
- Work around a refusal. `estimate` rejects sessions outside the declared sample sessions of a parliament that is not live, because only a live parliament is processed beyond its samples. Adding a session to `readiness.sample_sessions` is the user's decision.
- Write an attestation the user did not state, or change a threshold without their reason.
- Commit or push. The scorecard changes stay in the working tree.
- Copy anything from the private Research repository into this one.
