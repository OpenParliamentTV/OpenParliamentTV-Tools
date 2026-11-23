---
name: evaluate-all
description: Evaluate every implemented parliament for production readiness, and plan the outstanding Whisper transcription across all of them as one detached job that starts only after the user picked what goes in and confirmed its estimate.
disable-model-invocation: true
allowed-tools: Bash(.venv/bin/python -m optv.scripts.readiness check), Bash(.venv/bin/python -m optv.scripts.readiness check *), Bash(.venv/bin/python -m optv.scripts.readiness qc *), Bash(.venv/bin/python -m optv.scripts.readiness estimate *), Bash(.venv/bin/python -m optv.scripts.readiness catalogue), Bash(.venv/bin/python -m optv.scripts.readiness jobs), Bash(.venv/bin/python -m optv.scripts.readiness jobs list*), Bash(.venv/bin/python -m optv.scripts.readiness jobs show *), Bash(git status *)
---

# Evaluate all parliaments

This is `/evaluate-parliament` across every parliament. Read `.claude/skills/evaluate-parliament/SKILL.md` first: its steps, its rule that a job starts only on the user's answer to a shown estimate, and its list of what never happens all apply here unchanged. What differs is the scale, so the choice of what to run matters more.

Run everything from the repository root.

## Steps

1. **Score everything.** `.venv/bin/python -m optv.scripts.readiness check`. Give the overview table and how many parliaments sit at each level.

2. **Refresh existing figures.** For each parliament whose `qc.fidelity` is `stale` or `not_evaluated` with "no QC figures" in its detail, run `.venv/bin/python -m optv.scripts.readiness qc <CODE>`, then `check` again. Parliaments without any transcript need nothing here. Comparing takes about half a second per speech, so a parliament with many freshly transcribed sessions can take several minutes; say so before a long one.

3. **Estimate the whole backlog.** List the codes of all parliaments and pass them to one estimate: `.venv/bin/python -m optv.scripts.readiness estimate <CODE> <CODE> ...`. The result is one plan covering every session with clips still to transcribe.

4. **Let the user choose.** The full backlog is usually days of computing. Summarise it per parliament: sessions, hours of audio, expected hours, and which level 3 criteria that parliament would then have evidence for. Add the grand total and when it would end. Also list what cannot be planned ("audio not staged") per parliament. Then ask which parliaments to include, as a multiple choice when the question tool is available, with "none for now" as an option.

5. **Estimate the selection and confirm it.** Run `estimate` again with only the chosen codes; this is a new plan with its own token. Show its steps, total, range and end time, and ask for the go-ahead on exactly this plan. One job runs at a time and works through its steps in order, so a plan with several parliaments is the queue.

6. **Start, only on a yes**, with the command the second estimate printed. Report the job id and the expected end, point to `/evaluation-jobs`, and stop.

7. **Sum up the rest.** Across all parliaments, group the open level 2 criteria by cause, because the same cause often holds back several parliaments at once (for example speaker IDs below the threshold, or low-confidence speeches that would import with text). That tells the user where one fix moves many parliaments.
