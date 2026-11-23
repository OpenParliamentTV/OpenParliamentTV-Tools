---
name: parliament-status
description: Report how production-ready the parliament implementations and their data are, from the readiness scorecards. Use this whenever the user asks which parliaments are ready, what level or status a parliament has, what blocks a parliament from the next level or from going live, wants docs/READINESS.md or docs/readiness/*.json refreshed, or has changed a parliament's code, manifest or sample data and wants to see the effect. Also use it before answering any question about a parliament's quality from memory.
argument-hint: "[CODE ...]"
allowed-tools: Bash(.venv/bin/python -m optv.scripts.readiness check), Bash(.venv/bin/python -m optv.scripts.readiness check *), Bash(.venv/bin/python -m optv.scripts.readiness overview *), Bash(.venv/bin/python -m optv.scripts.readiness catalogue), Bash(.venv/bin/python -m optv.scripts.readiness qc *), Bash(.venv/bin/python -m optv.scripts.readiness jobs), Bash(.venv/bin/python -m optv.scripts.readiness jobs list*), Bash(.venv/bin/python -m optv.scripts.readiness jobs show *), Bash(git status *), Bash(git diff *)
---

# Parliament status

Readiness is tracked per parliament as a scorecard: about twenty criteria, each `pass`, `fail`, `not_evaluated`, `not_applicable` or `stale`, and the level they add up to. The scorecards are files in this repository, so the answer to "how ready is X" is always a fresh evaluation, not recollection.

| Level | Name | Meaning |
|---|---|---|
| 1 | implemented | a directory under `optv/parliaments/` exists |
| 2 | samples pass | the sample sessions satisfy every level 2 criterion |
| 3 | QC cleared | Whisper transcripts confirm the text, licence and platform fit are attested; ready for a backfill and for going live |
| 4 | backfilled | declared in the manifest |
| 5 | live | declared in the manifest |

Levels 1 to 4 are published as `in development`, level 5 as `production`. A parliament at a declared level (4 or 5) keeps it when a criterion below fails; the failure is listed as a regression.

## Steps

Run everything from the repository root. `$ARGUMENTS` holds the parliament codes the user named, if any.

1. Evaluate: `.venv/bin/python -m optv.scripts.readiness check $ARGUMENTS` (no codes means all parliaments). This takes about twenty seconds, reads only local files, and rewrites `docs/readiness/<CODE>.json` and `docs/READINESS.md` where a result changed.
2. Report what the user asked about:
   - For an overview, give the table the command printed: code, level, status, and what blocks the next level. Say how many parliaments sit at each level.
   - For specific parliaments, read `docs/readiness/<CODE>.json` and explain each criterion that is not satisfied in plain words, with its figures (for example "76 % of speeches have a speaker ID, the threshold is 95 %"). `docs/READINESS.md` has the rule behind each criterion.
3. Mention running or unfinished jobs: `.venv/bin/python -m optv.scripts.readiness jobs list`. A finished job whose results are not in the scorecard yet is worth pointing out; `/evaluation-jobs` handles it.
4. Say which files the evaluation changed (`git status --short docs/`). They are left uncommitted for the user to review.

## Reading the results

- `not_evaluated` on the `qc.*` criteria means the Whisper evidence does not exist yet. Producing it takes hours per session; that is what `/evaluate-parliament <CODE>` is for. Do not start it from here.
- `stale` means the evidence describes an earlier state of the data. For `qc.fidelity` and `qc.mismatch`, `.venv/bin/python -m optv.scripts.readiness qc <CODE>` recomputes the figures from the existing transcripts in a few minutes.
- `samples.import_gate` failing means speeches the merger marked as low confidence would still be imported with their text, because the platform only honours `debug.confidence` when `debug.linkedMediaIndexes` is present too.
- A criterion that reads "no data repository on this machine" kept its last recorded result; say so rather than presenting it as freshly measured.
- Attested criteria (`qc.reviewed`, `release.*`) are statements by a person, recorded under `readiness.attestations` in the manifest. Explain what would have to be stated; do not add an attestation unless the user states it.

## Boundaries

This skill reports. It does not start transcription jobs, run pipeline stages, change thresholds or sample sessions in a manifest, or commit anything. If the user wants a threshold changed for a parliament, that is an edit to `readiness.thresholds` in its manifest with the reason they give, followed by another `check`.

Nothing from the private Research repository belongs in the scorecards or in this repository.
