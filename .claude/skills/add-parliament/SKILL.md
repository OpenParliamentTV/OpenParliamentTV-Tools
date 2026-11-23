---
name: add-parliament
description: Onboard a new parliament into Open Parliament TV, from its Research profile to three sample sessions scored by the readiness tooling. Scaffolds the implementation and a local data repository, guides the scraper, parsers and merger through checkpoints, and stops before anything leaves the machine.
argument-hint: "<CODE> | next"
disable-model-invocation: true
allowed-tools: Bash(.venv/bin/python -m optv.scripts.readiness next), Bash(.venv/bin/python -m optv.scripts.readiness next *), Bash(.venv/bin/python -m optv.scripts.readiness scaffold * --dry-run), Bash(.venv/bin/python -m optv.scripts.readiness check *), Bash(.venv/bin/python -m optv.scripts.readiness estimate *), Bash(.venv/bin/python -m optv.scripts.readiness sync), Bash(.venv/bin/python -m optv.scripts.readiness jobs), Bash(.venv/bin/python -m optv.scripts.readiness jobs list*), Bash(.venv/bin/python -m optv.scripts.readiness jobs show *), Bash(.venv/bin/python -m optv.shared.validators.cli *), Bash(.venv/bin/pytest *), Bash(git status *), Bash(git diff *)
---

# Add a parliament

`$ARGUMENTS` is the shortcode of the parliament to add, for example `DK` or `DE-HE`, or the word `next` to have the most feasible parliament without an implementation chosen (section 0). The work ends when three sample sessions of it are processed end to end and scored, and the user holds a list of what is left for them to do. [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) is the reference for every step; read it before starting. This skill adds the order of work, the checkpoints, and the limits.

Three limits shape everything below.

- **Sample sessions only.** A parliament that is not live is processed for three sample sessions. Fetching, aligning or transcribing a whole period takes days and thousands of requests; it is never started here and not proposed as a next step.
- **Requests are counted before they are sent.** A parliament's servers are someone else's. Before any run that talks to them, say how many requests it will make and to which host, and get the user's go-ahead for anything beyond a handful. A burst of requests has had the user's address blocked before.
- **Nothing leaves the machine.** No GitHub repository is created, nothing is committed or pushed. Those steps are listed for the user at the end.

## 0. When the argument is `next`

```
.venv/bin/python -m optv.scripts.readiness next
```

It reads the Research repository's overview, which lists every surveyed parliament by feasibility score, highest first, and leaves out the ones implemented here. The first line is the candidate; the lines after it are the runners-up. Parliaments that were looked into and set aside are named separately and not offered.

- If it says profiles changed since the overview was generated, regenerate the overview first (`python3 scripts/update_overview.py` in the Research repository; a generated file there, left uncommitted) and run the command again. A stale overview can rank a parliament by a score its profile no longer has.
- Read the candidate's profile before proposing it, above all its blockers, its terms of use and when it was last verified. The score says how good the sources look on paper. A blocker the score does not weigh, terms that forbid republishing, or a profile verified long ago are reasons to say so and to look at the runner-up as well.
- Tell the user which parliament came out, its score with the three parts, the runners-up, and anything from the profile that speaks against it. Then ask which one to add. Adding a parliament is days of work, so the choice is theirs to confirm, and they may pick another. With equal scores the order is alphabetical by code, which is no reason to prefer one.

Continue with the confirmed shortcode. The scores and the ranking stay in the conversation; they are not written into this repository.

## 1. Check what exists

- The shortcode follows SHORTCODES.md (section 1 of the guide) and `optv/parliaments/<CODE>/` does not exist yet.
- The Research profile is at `../OpenParliamentTV-Research/parliaments/<CC>/<CODE>.md`, where `<CC>` is the country part of the code. Read it: video sources, proceedings sources, the joining strategy it proposes, blockers, and the terms of use. If there is no profile, stop and say so. The profile is where the source survey lives, and writing one follows the Research repository's own instructions.

The Research repository is private. Use the profile to decide what to build. Do not copy its text, scores or licence assessments into this repository; write the parliament's README and manifest from what the implementation does and from the public sources.

The profile is a survey, and implementing is the first time anyone works with the source in earnest. Expect to find it wrong in places, and treat that as part of the job: see "When the source contradicts the profile" below.

## 2. Agree the basics

Settle with the user, in one round of questions: the parliament's name as it should appear, the periods to cover, the language codes and spaCy model (propose them), the terms of use of the video recordings as the parliament publishes them (their wording or the address of the page; no parliament is added without them), and which three sessions are the samples. Good samples are recent, complete at the source, and ordinary: a typical sitting teaches more than an inaugural one. A fourth sample is for a known special case, when there is one.

## 3. Scaffold

```
.venv/bin/python -m optv.scripts.readiness scaffold <CODE> --name "<name>" \
    --type <national|regional|supranational> --language <iso3> \
    --language-code <iso2> --spacy-model <model> --period <N> [--period <N> ...] \
    --media-license "<terms of use of the video>" [--proceedings-license "<terms>"] --dry-run
```

Show the dry run, then run it without `--dry-run` (expect a permission prompt). It creates the parliament directory with a skeleton that imports and passes the contract tests while its scraper, parsers and merger still raise `NotImplementedError`, and a local data repository beside this one from the data-repository template, with the status line `in development`. It lists the parliament in the README's table of implemented parliaments, scores it once so that `docs/READINESS.md` shows it at level 1, reports whether the Conductor knows the parliament, and prints what is left.

Run `.venv/bin/pytest -q tests` once. It passes on a fresh scaffold; from here on a failure is yours.

## 4. Implement, one checkpoint at a time

Stop at each checkpoint and show the user what works, with real output, before going on. At each one, also say whether anything you met contradicts the Research profile.

The guide's section 5 lists the shared building blocks (polite HTTP, the media-spine merger frame, sequence alignment, PDF and subtitle text sources, agenda types, the confidence mark). Reach for those before writing a parliament's own version; the existing implementation closest to this source's shape shows how they fit together.

1. **Scraper** (`scraper/fetch.py`). Confine it to the requested sessions before the first request, by `args.limit_session` or a session flag of the parliament's own. State what an unconfined run would fetch. Use `optv.shared.http.PoliteHTTP`. Checkpoint: the raw files of the three samples are in `original/`, and you can say how many requests that took.
2. **Parsers** (`parsers/`). Checkpoint: `original/media/<session>-media.json` and `original/proceedings/<session>-proceedings.json` for the samples, and `Config.sessions()` lists the three.
3. **Merger** (`merger/merge_session.py`). Checkpoint: `cache/merged/<session>-merged.json`, and `.venv/bin/python -m optv.shared.validators.cli --file <file> --schema full` reports no error. Say how many media speeches found their text and how many did not.
4. **Tests.** Tiny synthetic fixtures under `tests/fixtures/<CODE>/` and a `tests/test_<code>*.py` for the parsers and the merger. Real source documents are not fixtures: they are large and not ours to redistribute.
5. **Workflow.** Run merge and entity linking for the samples, scoped, and look at a published file in `processed/` together with the user.
6. **README and manifest.** Fill the parliament's `README.md` (section 10 of the guide) and the manifest's licences and stages.

## When the source contradicts the profile

While implementing you will meet facts about the source: what a feed really contains, whether video is per speech or per session, how far back the archive goes, which identifiers join text to video, what the terms of use say. When one of them contradicts the profile, do two things.

**Flag it.** Tell the user at the next checkpoint, not at the end: what the profile says, what you found, how you checked it (the address fetched, what came back), and what it changes. A contradiction that changes the plan (no per-speech video after all, a join key that does not exist, terms that forbid republishing) is a reason to stop and decide together before building further.

**Correct the profile**, in the Research repository, by that repository's rules:

- Read `../OpenParliamentTV-Research/instructions/general.md` before editing. Its field definitions decide what a value means ("Extractable" is not "Yes"), and its format contract fixes sections, table rows and allowed values.
- Change the value and its one-clause note. Write the fact as it is now; the profile carries no history ("corrected", "previously"). Git shows the change.
- When a Data Availability value changes, the feasibility criteria that rest on it change too: recompute them, the breakdown's total and the Summary's Integration Feasibility as the instructions' scoring section says.
- Set `Last updated:` in the header comment to today's date. Leave `Verified:` alone: it is the date of the last full verification, and a correction is not one.
- Write only what you verified by visiting the source, without hedges. What you could not check stays as it was; mention it to the user instead.
- Run `python3 scripts/lint_profile.py <CODE>` in the Research repository until it reports 0 errors, then `python3 scripts/update_overview.py`.

Leave the edits uncommitted there and list them in your closing summary, each with what it said before. If several facts were wrong, or the profile reads as written for a source that has since been replaced, say so: the fix for that is a full re-verification by the Research repository's own procedure, which is the user's call, not a string of corrections from here.

The flow of information is one way. Findings go into the profile; nothing from the profile goes into this repository.

## 5. Declare the samples

In the manifest's `readiness` block, list the three sessions under `sample_sessions`. Then declare `sample_run`: the workflow arguments that confine a run to those sessions, downloads included.

```yaml
readiness:
  sample_sessions: ["<a>", "<b>", "<c>"]
  sample_run:
    - sessions: ["<a>", "<b>", "<c>"]
      args: ["--period=<N>", "--limit-session", "^(<a>|<b>|<c>)$"]
```

Declare it only after reading the download hook once more and confirming the arguments hold there, because the tooling trusts this entry: it is the one thing that lets a job run the workflow with downloads. Tell the user what you verified.

## 6. The long runs

Alignment needs each speech's audio, which means downloading and cutting media. That is the long part, and it goes through a job like every long run:

```
.venv/bin/python -m optv.scripts.readiness estimate <CODE> --sample-run [--ner-api-endpoint <url>]
```

Show the plan: the exact command, the sessions, the stages, that it downloads, and the expected duration. The first sample run of a parliament has no earlier run to estimate from and the plan says so; say it too, rather than inventing a figure. Start it only on the user's answer to that plan, with the command the estimate printed. It runs detached; `/evaluation-jobs` picks it up.

After it finished, `/evaluate-parliament <CODE>` plans the Whisper transcription of the samples, with its own estimate and confirmation.

## 7. Score and close

- `.venv/bin/python -m optv.scripts.readiness check <CODE>`, then go through what blocks level 2 with the user.
- `.venv/bin/python -m optv.scripts.readiness sync` shows where the status entries differ. The scaffold put the parliament into the README table of implemented parliaments; if `sync` reports it missing there, add the row. `sync --apply` then sets the status in the data repository's README and in the Research profile.
- In the Research profile, under Notes, write or update `### OPTV implementation`: that an adapter exists, its status and data coverage, and which source it uses, in one or two sentences as the Research instructions ask. Code structure does not belong there. Then lint the profile and regenerate the overview as above.
- Conductor: the scaffold printed a block for `config/parliaments.yaml`. That file, the compose override that mounts the data repository, and the restart belong to the Conductor checkout; make those edits only with the user's go-ahead.

End with the list for the user: create the GitHub repository `OpenParliamentTV/OpenParliamentTV-Data-<CODE>`, add it as the remote of the local data repository, review and commit the repositories, push. Say which files you changed in which repository, and list every correction you made to the Research profile with what it said before.

## What this skill never does

- Fetch, align or transcribe beyond the sample sessions, or run a workflow stage without the arguments that confine it.
- Send a burst of requests without the user's go-ahead for its size.
- Start a job without the user's answer to its estimate.
- Create a repository on GitHub, commit, or push.
- Copy content of the Research profile into this repository.
- Build on a fact the profile states once the source has shown it to be wrong, or leave the profile saying it.
- Write an attestation or lower a threshold to make the scorecard look better.
