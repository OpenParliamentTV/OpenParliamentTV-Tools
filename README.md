# Open Parliament TV - Tools

The data import pipeline for Open Parliament TV. Fetches parliamentary proceedings and media feeds, parses them into a unified per-session JSON file, enriches with named-entity linking, sentence-level audio alignment, and named-entity recognition, then validates and publishes the result for the platform to ingest.

For the wider system context — repositories, data flow, the Stage 2 format — see the [Architecture repo](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture). The pipeline stages map to [PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md); the file format produced by the pipeline is specified in [STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md).

Currently implemented: the German Bundestag (`optv/parliaments/DE/`).

## Quick start

```bash
python3 -m pip install -r requirements.txt

# fetch + process the current period's data into <data_dir>
./optv/parliaments/DE/update <data_dir>

# or run the workflow manually with finer control:
./optv/parliaments/DE/workflow.py --period=21 <data_dir> \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

`<data_dir>` is the per-parliament data directory, expected to be a sibling clone of [OpenParliamentTV-Data-DE](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE). Each `--*` flag is opt-in and idempotent; `--force` re-runs an already-completed stage. A lockfile (`<data_dir>/optv.lock`) blocks concurrent runs.

External dependencies: `aeneas` needs `ffmpeg` and `espeak`; the NER stage needs a spaCy model (declared per-parliament in `manifest.yaml` as `locale.spacy_model` — `de_core_news_md` for DE) and an `entityfishing` API endpoint passed via `--ner-api-endpoint`.

## Layout

```
optv/
├── parliaments/
│   └── DE/                  # German Bundestag — only currently implemented parliament
│       ├── manifest.yaml    # per-parliament metadata read by Conductor (stages, periods, …)
│       ├── workflow.py      # thin entry point — defines hooks, calls optv.shared.workflow
│       ├── common.py        # Config class (paths + file naming); re-exports shared helpers
│       ├── scraper/         # fetch proceedings (TEI XML) and media (RSS)
│       ├── parsers/         # XML/RSS → intermediate JSON
│       ├── merger/          # join media + proceedings into Stage 2
│       ├── update           # shell wrapper: --period=21 --retry-count=20
│       └── Makefile         # download + merge targets driven by file mtimes
└── shared/                  # cross-parliament infrastructure
    ├── workflow.py          # stage orchestrator + WorkflowHooks + shared argparser
    ├── publish.py           # non-destructive publish helpers (demotion guard, carry-forward)
    ├── session_status.py    # SessionStatus enum
    ├── align.py             # forced sentence alignment (aeneas)
    ├── nel.py               # named-entity linking (Wikidata)
    ├── ner.py               # named-entity recognition (spaCy + entity-fishing)
    ├── agenda_types.py      # cross-parliament agenda-type vocabulary
    ├── schema/              # Stage 2 JSON schemas + reference doc
    ├── validators/          # structural + semantic validators, CLI
    └── docs/EXAMPLES/       # example Stage 2 documents
```

`workflow.py` is intentionally thin: orchestration (merge → NEL → align → NER → publish), the common argparser, lockfile handling and the publish helper all live in [`optv/shared/workflow.py`](optv/shared/workflow.py). The per-parliament file only contains the four adapters that legitimately differ — download, parse, merge call shape, align call shape — plus parliament-specific CLI flags.

`manifest.yaml` is the per-parliament metadata file. The Conductor reads it to know which stages a parliament supports, which entity dump to use, and the retry defaults — see [optv/parliaments/DE/manifest.yaml](optv/parliaments/DE/manifest.yaml) for the canonical example.

## Pipeline stages

Each stage produces a side-by-side cache file per session (e.g. `21001-merged.json`, `21001-aligned.json`, `21001-ner.json`) and runs only when its input is newer than its output.

| Stage | Module / script | Input | Output |
|-------|-----------------|-------|--------|
| Fetch | [`scraper/fetch_proceedings.py`](optv/parliaments/DE/scraper/fetch_proceedings.py), [`scraper/fetch_media.py`](optv/parliaments/DE/scraper/fetch_media.py) | parliament APIs | `original/{proceedings,media}/` |
| Parse | [`parsers/proceedings2json.py`](optv/parliaments/DE/parsers/proceedings2json.py), [`parsers/media2json.py`](optv/parliaments/DE/parsers/media2json.py) | TEI XML, RSS | intermediate JSON |
| Merge | [`merger/merge_session.py`](optv/parliaments/DE/merger/merge_session.py) | proceedings + media JSON | `cache/merged/*-merged.json` |
| NEL | [`optv/shared/nel.py`](optv/shared/nel.py) | merged JSON + entity dump | `people[].wid`, faction normalisation |
| Align | [`optv/shared/align.py`](optv/shared/align.py) | merged JSON + audio | `cache/aligned/*-aligned.json` (sentence timings) |
| NER | [`optv/shared/ner.py`](optv/shared/ner.py) | aligned JSON + entity-fishing API | `cache/ner/*-ner.json` (sentence entities) |
| Publish | `_publish_as_processed()` in [`optv/shared/workflow.py`](optv/shared/workflow.py) (uses helpers from [`optv/shared/publish.py`](optv/shared/publish.py)) | latest cache file | `processed/*-session.json` |

For the conceptual stage breakdown (parliament-agnostic), see [Architecture/PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md).

## Validation

Stage 2 schemas and conventions: [optv/shared/schema/README.md](optv/shared/schema/README.md). Standalone CLI:

```bash
python -m optv.shared.validators.cli --dir <data_dir>/processed --schema full
python -m optv.shared.validators.cli --file session.json --no-semantic
```

The publish step also runs validation and logs findings; warnings do not block.

## Tests

```bash
.venv/bin/pytest -q tests
# or
make -C optv/parliaments/DE test
```

Flat layout under [tests/](tests/), no config files. Covers pure helpers, the merger contract that catches `originID`/`originTextID`-class regressions, the agenda-type vocabulary, and an end-to-end smoke run on tiny synthetic fixtures. Stages requiring external services (aeneas alignment, NER, NEL endpoint) are not in the suite. CI runs the same command on every push and PR via [.github/workflows/tests.yml](.github/workflows/tests.yml).

## Adding a new parliament

See [docs/ADDING-A-PARLIAMENT.md](docs/ADDING-A-PARLIAMENT.md).
