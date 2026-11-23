---
name: evaluation-jobs
description: Check on the detached Whisper evaluation jobs of the readiness tooling, and turn a finished job into results. Use this whenever the user asks how a transcription or evaluation job is doing, whether it finished, why it stopped, or what came out of it; when a job was interrupted by a reboot or a closed lid and needs resuming; and after any job finished, to fold its evidence into the scorecards and review the Whisper reports.
allowed-tools: Bash(.venv/bin/python -m optv.scripts.readiness jobs), Bash(.venv/bin/python -m optv.scripts.readiness jobs list*), Bash(.venv/bin/python -m optv.scripts.readiness jobs show *), Bash(.venv/bin/python -m optv.scripts.readiness jobs logs *), Bash(.venv/bin/python -m optv.scripts.readiness check *), Bash(.venv/bin/python -m optv.scripts.readiness qc *), Bash(git status *)
---

# Evaluation jobs

A job transcribes the sample sessions of one or more parliaments with Whisper and compares each transcript with the published text. It runs detached: it was started once, after the user confirmed its estimate, and keeps going without any session watching. This skill is how a later session picks it up again.

Jobs live outside the repositories, in `~/.local/state/optv-readiness/jobs/<id>/` (plan, state, logs). Their evidence lands in `cache/whisper/` of each data repository: `<session>-whisper.json` (transcripts), `<session>-qc.json` (figures) and `<session>-report.md` (the readable comparison).

Run everything from the repository root.

## Steps

1. **List.** `.venv/bin/python -m optv.scripts.readiness jobs list`, then `jobs show <ID>` for each job that is not long finished.

2. **Act on the status.**

   | Status | What it means | What to do |
   |---|---|---|
   | `planned` | estimated, never started | nothing; an estimate older than a day has to be redone via `/evaluate-parliament` |
   | `running` | the runner is alive | report the step in progress, the clips done, the time left and the expected end |
   | `interrupted` | the state says running but no runner is alive: reboot, crash, or kill | explain, then offer `jobs resume <ID>`; finished steps are not repeated and finished speeches are kept |
   | `cancelled` | stopped on request | resumable the same way |
   | `done` | every step's result is on disk | go to step 3 |
   | `done_with_failures`, `failed` | some or all steps did not produce their result | `jobs logs <ID>`, find the cause, then step 3 for the steps that did finish |

   `jobs resume` and `jobs cancel` change what runs on this machine for hours, so ask the user before either; expect a permission prompt as well. A closed laptop lid only pauses a job, which continues on wake; that is not an interruption.

   A step fails when its result is missing, whatever its exit code. The usual causes: a speech hit its time limit twice (the transcript has `"ok": false, "attempts": 2` for it, and it is not tried again until someone runs `.venv/bin/python -m optv.shared.whisper_diff transcribe --data-dir <dir> --session <S> --speech <N> --force`), or clips went missing. Report failed speeches by session and index; a handful of failures still leaves coverage above its threshold.

3. **Fold the evidence in.** For every parliament a finished job touched: `.venv/bin/python -m optv.scripts.readiness qc <CODE>` (a no-op where the job's own `diff` steps already wrote the figures), then `check <CODE>`. Report how `qc.whisper_coverage`, `qc.fidelity` and `qc.mismatch` came out, with the per-session medians from `docs/readiness/<CODE>.json`.

4. **Review the reports.** The figures say how much of the text matches; they do not say why the rest does not. For each newly evaluated session read `cache/whisper/<session>-report.md` and the rows of `<session>-qc.json`, classify the suspect speeches with `rubric.md` in this skill's directory, and summarise per parliament: how many speeches fall in each class, which of them the platform would import with text, and what in the parser or merger would explain the pattern. Quote speech indexes so the user can look.

5. **Say what is still open for level 3.** `qc.reviewed` is the user's statement that they went through the reports of the current evidence. It is recorded in the manifest as `readiness.attestations.qc_reviewed` with the `qc_evidence` value from the scorecard, and it goes stale when the text or the transcripts change. Offer to record it only once the user says they reviewed the findings; your own review in step 4 prepares theirs, it does not replace it.

## Boundaries

- Starting a new job belongs to `/evaluate-parliament` or `/evaluate-all`, with their estimate and confirmation.
- No pipeline stages, no downloads, no commits. `check` rewrites `docs/readiness/` and `docs/READINESS.md`; those changes stay in the working tree.
- Findings about a parliament's text quality belong in the summary for the user. Nothing from the private Research repository goes into this one.
