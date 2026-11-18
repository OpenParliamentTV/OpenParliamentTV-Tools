# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

Parses parliamentary proceedings and media feeds into a standardized "Stage 2" JSON format consumed by the OpenParliamentTV platform. Multiple parliaments are implemented under `optv/parliaments/<CODE>/` (the German Bundestag `optv/parliaments/DE` is the reference and the only one live in production); `optv/shared/` holds cross-parliament logic. New parliaments follow [docs/ADDING-A-PARLIAMENT.md](docs/ADDING-A-PARLIAMENT.md).

## Environment

- Python 3 with the deps in `requirements.txt` (`feedparser`, `lxml`, `spacy`, `click`, `spacyfishing`, `numpy`, `aeneas`, `pymupdf`, `pyyaml`, `jsonschema`, `pytest`).
- `aeneas` (time alignment) requires ffmpeg + espeak.
- The NER and alignment stages read locale config from each parliament's `manifest.yaml` (`locale.spacy_model`, `locale.aeneas_language`, `locale.entityfishing_language`); per-parliament `workflow.py` injects these onto `args` via `optv.shared.workflow.inject_locale()` (which reads `optv.parliaments.get_locale()`) before invoking shared stages. NER additionally needs an `entityfishing` API endpoint passed via `--ner-api-endpoint`. For DE the spaCy model is `de_core_news_md`. A manifest's `ner: {corrections: true}` turns on [optv/shared/ner_corrections.py](optv/shared/ner_corrections.py) (stop mentions per language, excluded Wikidata classes, faction/speaker IDs from the entity dump); the lists live in `optv/shared/ner_lists/`, and [docs/NER-CORRECTIONS.md](docs/NER-CORRECTIONS.md) says whether a fix belongs there or in the platform's EntityRemap.
- Data lives **outside the repo**: each parliament has a sibling data repo (`OpenParliamentTV-Data-<ID>`), passed to the workflow as `<data_dir>`. The shared `BaseConfig` class in [optv/shared/config.py](optv/shared/config.py) defines the canonical layout (`original/{media,proceedings}`, `cache/{merged,aligned,ner}`, `processed/`, `metadata/`), which each parliament's `common.py` `Config` subclasses (re-exporting `SessionStatus` + publish helpers).

## Running the pipeline

Main entry point — [optv/parliaments/DE/workflow.py](optv/parliaments/DE/workflow.py):

```
./optv/parliaments/DE/workflow.py --period=21 <data_dir> \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

Each `--*` step is opt-in; `--force` re-runs an already-completed stage; `--limit-session <regex>` / `--no-limit-to-period` scope the work. A lockfile (`<data_dir>/optv.lock`) blocks concurrent runs unless `--no-single-instance` is passed.

`--period` defaults to the newest period in the parliament's `manifest.yaml`, `--retry-count` to its `default_retry_count`.

Individual stage scripts are directly runnable (all use `argparse`):
- [optv/parliaments/DE/scraper/fetch_proceedings.py](optv/parliaments/DE/scraper/fetch_proceedings.py), `update_media.py`, `fetch_media.py`
- [optv/parliaments/DE/parsers/proceedings2json.py](optv/parliaments/DE/parsers/proceedings2json.py), `media2json.py`
- [optv/parliaments/DE/merger/merge_session.py](optv/parliaments/DE/merger/merge_session.py)
- [optv/shared/align.py](optv/shared/align.py), [optv/shared/nel.py](optv/shared/nel.py), [optv/shared/ner.py](optv/shared/ner.py)

## Validation

Stage 2 JSON Schemas live in [optv/shared/schema/](optv/shared/schema/) (minimal vs full). The `workflow.py` publish step runs `validate_stage2(doc, schema="full", semantic=True)` and logs findings — errors do **not** block publishing (warning-only). Standalone:

```
python -m optv.shared.validators.cli --dir <data_dir>/processed --schema full
python -m optv.shared.validators.cli --file session.json --no-semantic
```

Schema validation is structural; cross-field invariants (date ordering, `speechIndex` monotonicity, wid format, speaker contexts, URL vs path) live in [optv/shared/validators/semantic_validator.py](optv/shared/validators/semantic_validator.py). See [optv/shared/schema/README.md](optv/shared/schema/README.md) for rule rationale.

## Tests

Pytest suite under [tests/](tests/) — flat layout, no config files. Run with `.venv/bin/pytest -q tests`. The suite covers pure helpers, the merger contract (the `originID`/`originTextID` regression class), the agenda-type vocabulary, and an end-to-end smoke run on tiny synthetic fixtures in `tests/fixtures/DE/`. [tests/test_conductor_contract.py](tests/test_conductor_contract.py) pins the surface the Conductor depends on for every parliament (argv, `common.Config`, manifest keys, progress log lines). Stages requiring external services (aeneas, NER, NEL) are not exercised. Cron does not import pytest, so the Pi is unaffected when tests fail.

## Output equivalence

Before merging a refactor of shared or parliament code, prove it leaves output untouched with [optv/scripts/equivalence.py](optv/scripts/equivalence.py): `snapshot` runs each parliament's real workflow (parse, merge, publish) offline on a scratch copy of its sibling data repo and records output digests; take one snapshot with `--tools` on the reference checkout and one on the changed checkout, then `compare`. It does not cover scrapers, alignment, NER or the entity-dump builders.

## Architecture notes

**Pipeline stages produce side-by-side files per session** (e.g. `21001-merged.json`, `21001-aligned.json`, `21001-ner.json`, `21001-session.json`). Each stage is driven by mtime comparison (`Config.is_newer`) and only runs when its input is newer than its output. "Publishing" = copying the latest valid cache file into `processed/<session>-session.json` (the platform's input). The publish helper in [optv/shared/workflow.py](optv/shared/workflow.py) refuses to demote a richer published file (an aligned/NER session must never be overwritten by a bare merge) and carries already-published per-speech enrichments (`people[].wid`, `agendaItem.type`, `debug.confidence`, …) forward via the helpers in [optv/shared/publish.py](optv/shared/publish.py) — so a stale worker can never silently strip data a newer worker had already produced.

**Session state is derived from file contents**, not a separate manifest. `Config.status()` returns a `set[SessionStatus]` computed by peeking at each stage's output (`debug.alignDuration`, `people[].wid`, etc.). When adding a new processing stage, emit a distinguishing `debug.*` key and teach `status()` about it.

**Workflow runner is shared**: per-parliament `workflow.py` is a thin wrapper that defines `WorkflowHooks` (download / parse / merge / align adapters) and calls `optv.shared.workflow.run_main(parliament_id, hooks, description=..., config_cls=Config)`. `run_main` builds the common argparser (incl. shared `--period`/`--lang`/`--retry-count`/`--retry-delay-max`, defaulted from the manifest), sets up logging, injects locale + manifest defaults, acquires the lockfile and drives `run_workflow` (the stage orchestration: merge → NEL → align → NER → publish). A hook set may name a `rebuild_queue` file in the cache directory; the runner then re-runs the sessions listed there with rebuild semantics after the regular stages (DE-HH, TW). Adding a new pipeline-wide stage means editing the shared runner; only genuinely parliament-specific deltas (extra flags via an `add_arguments` callback, the hook bodies) belong in the wrapper.

**Package layout quirk**: `workflow.py` supports both `python -m` and `./workflow.py` invocation via a `sys.path` bootstrap at the top — `optv.shared.*` must resolve in both cases. Keep that bootstrap when adding new entry points that import from `optv.shared`.

**German state-parliament text+align path (experimental)**: the DE *Landtage* (DE-BW/BY/HH/SH/SN) have a per-speech video spine but PDF-only proceedings. [optv/shared/pdf2tei/](optv/shared/pdf2tei/) parses those PDFs into ParlaMint-style TEI and the merger joins the text onto the spine via `spine_join.join_text_to_spine`. DE-NW parses the HTML version of its protocol in its own `parsers/proceedings2json.py` (one turn per spine speech, in the `pdf2tei.merge` body shape) and joins it with the same function, per agenda item. DE-NI is the analogue but joins time-aligned WebVTT via `attach_text_by_index`, so it needs no aeneas. Alignment audio is staged by [optv/shared/audio_prep.py](optv/shared/audio_prep.py): the generic `prepare_per_speech_audio` driver downloads each session/clip stream once and slices per-speech MP3s into the cache path `align_audio` expects; each parliament supplies a tiny `align_prep.py` adapter (the `#t=start,end` parliaments just call `make_fragment_prepare(hls=…)`; DE-BY transcodes its own per-speech clip), and `make_align_hook(prepare)` builds the shared `align_session_to_file` hook. **This whole text/align tier is a prototype: unvalidated (no Whisper-QC/text-fidelity audit has cleared it) and not ready for platform integration — keep docs honest about that.**

**Faction shape gotcha** (see recent commits `2bcffda`, `db10951`): `people[].faction` is sometimes a string and sometimes a dict `{label, wid, wtype}`. The NEL step in [optv/shared/nel.py](optv/shared/nel.py) normalizes to dict form. Any code reading `faction` must handle both or run post-NEL.

**Stage 2 output** wraps data in `{"meta": {...}, "data": [...]}`. The `meta.processing` sub-dict records stage timestamps; equality checks in `save_if_changed` / `_publish_as_processed` use a blake2b signature of the `data` array only so re-running a stage with no content change is a no-op.

## Conventions

- Scripts use `logging` (never `print` for status) — `--debug` raises to DEBUG level.
- Keep JSON output deterministic: `json.dump(..., indent=2, ensure_ascii=False)`.
- Do not commit files under `_planning/` (gitignored; local scratch space).
- Do not commit files in the data directory — it is a sibling repo (`OpenParliamentTV-Data-DE`).
- Maintenance / one-off / backfill / audit scripts go in one of two places: truly throwaway ones in `_planning/` (gitignored), reusable-and-tracked ones in [optv/scripts/](optv/scripts/) (run via `python -m optv.scripts.<name>`). Do not scatter them into `optv/shared/` or a parliament dir.
