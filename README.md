# Open Parliament TV - Tools

The data import pipeline for Open Parliament TV. Fetches parliamentary proceedings and media feeds, parses them into a unified per-session JSON file, enriches with named-entity linking, sentence-level audio alignment, and named-entity recognition, then validates and publishes the result for the platform to ingest.

For the wider system context (repositories, data flow, the Stage 2 format) see the [Architecture repo](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture). The pipeline stages map to [PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md); the file format produced by the pipeline is specified in [STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md); the structural differences across the implemented parliaments are surveyed in [DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

Each implemented parliament lives under `optv/parliaments/<CODE>/` and carries its own README documenting that pipeline's data model, merge strategy, runtime flags, and known limitations. New parliaments follow [`docs/ADDING-A-PARLIAMENT.md`](docs/ADDING-A-PARLIAMENT.md).

## Quick start

```bash
python3 -m pip install -r requirements.txt

# fetch + process the current period's data into <data_dir>
# (example shown for DE — every parliament has the same wrapper shape)
./optv/parliaments/DE/update <data_dir>

# or run the workflow manually with finer control:
./optv/parliaments/DE/workflow.py --period=21 <data_dir> \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

`<data_dir>` is the per-parliament data directory, expected to be a sibling clone of the parliament's data repo (for example [OpenParliamentTV-Data-DE](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE) for `DE`). Each `--*` flag is opt-in and idempotent; `--force` re-runs an already-completed stage. A lockfile (`<data_dir>/optv.lock`) blocks concurrent runs.

`pip install -r requirements.txt` installs the Python packages. Beyond that, the optional enrichment stages need prerequisites pip can't provide — and only for parliaments that run them (each declares its set in `manifest.yaml` as `supported_stages`):

- **Align** (`aeneas`) — the system binaries `ffmpeg` and `espeak`.
- **NER** — a spaCy model (per-parliament `locale.spacy_model`, e.g. `de_core_news_md`, installed via `python -m spacy download <model>`) and a running `entityfishing` service (passed as `--ner-api-endpoint`).

The video-only parliaments (`download, parse, merge, nel`) need none of these.

## Layout

```
optv/
├── parliaments/
│   └── <CODE>/              # one directory per implemented parliament
│       ├── manifest.yaml    # per-parliament metadata read by Conductor (stages, periods, …)
│       ├── workflow.py      # thin entry point — defines hooks, calls optv.shared.workflow
│       ├── common.py        # Config class (paths + file naming); re-exports shared helpers
│       ├── scraper/         # fetch raw proceedings + media
│       ├── parsers/         # native format → intermediate JSON
│       ├── merger/          # join media + proceedings into Stage 2
│       ├── update           # shell wrapper with parliament-specific defaults
│       └── Makefile         # download + merge targets driven by file mtimes
└── shared/                  # cross-parliament infrastructure
    ├── workflow.py          # run_main + stage orchestrator + WorkflowHooks + shared argparser
    ├── config.py            # BaseConfig (on-disk layout, status/mtime helpers)
    ├── publish.py           # non-destructive publish helpers (demotion guard, carry-forward)
    ├── session_status.py    # SessionStatus enum
    ├── speech_id.py         # speech-id model normalizer (originID/originMediaID/originTextID)
    ├── merge_format.py      # generic merge formatting helpers (slug, offsets, name split)
    ├── lang/                # language-specific text helpers, keyed by ISO 639-1 (de, …)
    ├── align.py             # forced sentence alignment (aeneas)
    ├── nel.py               # named-entity linking (Wikidata)
    ├── ner.py               # named-entity recognition (spaCy + entity-fishing)
    ├── agenda_types.py      # cross-parliament agenda-type vocabulary + classifier registry
    ├── entity_dump_bootstrap.py  # temporary pre-platform Wikidata entity-dump builder
    ├── schema/              # Stage 2 JSON schemas + reference doc
    ├── validators/          # structural + semantic validators, CLI
    └── docs/EXAMPLES/       # example Stage 2 documents
```

`workflow.py` is intentionally thin: orchestration (merge → NEL → align → NER → publish), the common argparser, lockfile handling and the publish helper all live in [`optv/shared/workflow.py`](optv/shared/workflow.py). The per-parliament file only contains the four adapters that legitimately differ — download, parse, merge call shape, align call shape — plus parliament-specific CLI flags.

`manifest.yaml` is the per-parliament metadata file. The Conductor reads it to know which stages a parliament supports, which entity dump to use, and the retry defaults — see [optv/parliaments/DE/manifest.yaml](optv/parliaments/DE/manifest.yaml) for the canonical example.

## Pipeline stages

Each stage produces a side-by-side cache file per session (e.g. `21001-merged.json`, `21001-aligned.json`, `21001-ner.json`) and runs only when its input is newer than its output. Paths below link to DE as the worked example; every parliament directory carries the same shape — see its README for parliament-specific module names and source formats. Which stages a parliament runs is declared in its `manifest.yaml` `supported_stages`; several are video-only and stop after NEL (no align/NER).

| Stage | Module / script | Input | Output |
|-------|-----------------|-------|--------|
| Fetch | [`scraper/fetch_proceedings.py`](optv/parliaments/DE/scraper/fetch_proceedings.py), [`scraper/fetch_media.py`](optv/parliaments/DE/scraper/fetch_media.py) | parliament APIs | `original/{proceedings,media}/` |
| Parse | [`parsers/proceedings2json.py`](optv/parliaments/DE/parsers/proceedings2json.py), [`parsers/media2json.py`](optv/parliaments/DE/parsers/media2json.py) | native formats (DE: TEI XML + RSS) | intermediate JSON |
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

Flat layout under [tests/](tests/), no config files. Covers pure helpers, the merger contract that catches `originID`/`originTextID`-class regressions, the agenda-type vocabulary, and an end-to-end smoke run on tiny synthetic fixtures. Stages that need external data or services (aeneas alignment, the NEL entity dump, entity-fishing NER) are not in the suite. CI runs the same command on every push and PR via [.github/workflows/tests.yml](.github/workflows/tests.yml).

## Quality control

An opt-in QC toolset cross-checks output without touching pipeline data. It has its own dependencies (`pip install -r requirements-qc.txt` — faster-whisper, Resemblyzer, torch) and is not part of the production pipeline:

- [`optv/shared/whisper_diff.py`](optv/shared/whisper_diff.py) (`rank` / `transcribe` / `diff`) — transcribes a session's audio with faster-whisper (+ optional Resemblyzer speaker-change detection, via [`whisper_qc.py`](optv/shared/whisper_qc.py)) and reports where it diverges from the proceedings text.
- [`optv/shared/merge_audit.py`](optv/shared/merge_audit.py) — read-only sweep over merger output that flags text-accumulation anomalies (histograms + suspect lists).

## Implemented parliaments

`DE` (Deutscher Bundestag) is the reference implementation and the only parliament in production; the others are in development. For how their data structures differ, see [DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

| Parliament | Name | Type | Data Repository | Status |
|------------|------|------|------|--------|
| [`DE`](optv/parliaments/DE/) | Deutscher Bundestag | national | [Data-DE](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE) | **production** |
| [`DE-BW`](optv/parliaments/DE-BW/) | Landtag von Baden-Württemberg | regional | [Data-DE-BW](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-BW) | in development |
| [`DE-BY`](optv/parliaments/DE-BY/) | Bayerischer Landtag | regional | [Data-DE-BY](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-BY) | in development |
| [`DE-HH`](optv/parliaments/DE-HH/) | Hamburgische Bürgerschaft | regional | [Data-DE-HH](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-HH) | in development |
| [`DE-NI`](optv/parliaments/DE-NI/) | Niedersächsischer Landtag | regional | [Data-DE-NI](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-NI) | in development |
| [`DE-NW`](optv/parliaments/DE-NW/) | Landtag Nordrhein-Westfalen | regional | [Data-DE-NW](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-NW) | in development |
| [`DE-RP`](optv/parliaments/DE-RP/) | Landtag Rheinland-Pfalz | regional | [Data-DE-RP](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-RP) | in development |
| [`DE-SH`](optv/parliaments/DE-SH/) | Landtag Schleswig-Holstein | regional | [Data-DE-SH](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-SH) | in development |
| [`DE-SN`](optv/parliaments/DE-SN/) | Sächsischer Landtag | regional | [Data-DE-SN](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-SN) | in development |
| [`DE-ST`](optv/parliaments/DE-ST/) | Landtag von Sachsen-Anhalt | regional | [Data-DE-ST](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE-ST) | in development |
| [`ES`](optv/parliaments/ES/) | Congreso de los Diputados | national | [Data-ES](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-ES) | in development |
| [`EU`](optv/parliaments/EU/) | European Parliament | supranational | [Data-EU](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-EU) | in development |
| [`FI`](optv/parliaments/FI/) | Eduskunta | national | [Data-FI](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-FI) | in development |
| [`FR`](optv/parliaments/FR/) | Assemblée nationale | national | [Data-FR](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-FR) | in development |
| [`NO`](optv/parliaments/NO/) | Stortinget | national | [Data-NO](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-NO) | in development |
| [`PT`](optv/parliaments/PT/) | Assembleia da República | national | [Data-PT](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-PT) | in development |
| [`SE`](optv/parliaments/SE/) | Sveriges Riksdag | national | [Data-SE](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-SE) | in development |
| [`TW`](optv/parliaments/TW/) | Legislative Yuan (立法院) | national | [Data-TW](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-TW) | in development |

## Adding a new parliament

See [docs/ADDING-A-PARLIAMENT.md](docs/ADDING-A-PARLIAMENT.md).
