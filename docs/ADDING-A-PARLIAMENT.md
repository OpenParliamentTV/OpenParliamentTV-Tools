# Adding a parliament

This is the end-to-end checklist for onboarding a new parliament into OpenParliamentTV. The German Bundestag implementation in [optv/parliaments/DE/](../optv/parliaments/DE/) is the reference; new parliaments follow the same shape.

For the data contract and pipeline concepts, see [Architecture/STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md) and [Architecture/PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md).

---

## 1. Choose a shortcode

Follow [Architecture/SHORTCODES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/SHORTCODES.md). National parliaments use the country's ISO 3166 alpha-2 code (`DE`, `SE`, `FR`); regional parliaments use the ISO subdivision code (`DE-BE`, `DE-BY`); custom codes are allowed where ISO doesn't fit (`US-NYC`, `CAT`). Always uppercase.

The shortcode becomes the directory name (`optv/parliaments/<CODE>/`) and the data-repo suffix (`OpenParliamentTV-Data-<CODE>`).

## 2. Survey the source data

Before writing any code, confirm the parliament publishes the four things the pipeline needs:

- **Video** for every speech, fetchable by URL. Either per-speech files or a session video plus per-speech timecodes. Without accessible video, the parliament is not a viable target.
- **Proceedings / transcripts** in some structured format (TEI XML, ParlaMint, Akoma Ntoso, custom JSON, etc.).
- **A media metadata feed** (RSS, JSON API, …) listing video items with at least the parliament's internal media ID and a public source page.
- **Wikidata coverage** for at least the major speakers. Not strictly required (the NEL stage tolerates gaps), but coverage gaps degrade platform features.

Validation target for the eventual output: [optv/shared/schema/stage2-full.schema.json](../optv/shared/schema/stage2-full.schema.json). Read [optv/shared/schema/README.md](../optv/shared/schema/README.md) before designing the parser — it documents enum values, datetime patterns, and conventions that the parser must satisfy.

## 3. Set up the directory

Copy `optv/parliaments/DE/` as a template:

```
optv/parliaments/<CODE>/
├── __init__.py
├── manifest.yaml
├── workflow.py          # entry point
├── common.py            # Config class, paths, file naming
├── scraper/             # fetch raw proceedings + media
├── parsers/             # parliament's native format → intermediate JSON
├── merger/              # join proceedings + media → Stage 2
├── Makefile             # convenience targets driven by file mtimes
└── update               # shell wrapper baking in default flags
```

The package layout matters: `workflow.py` imports from `optv.shared.*`, and the path bootstrap at the top supports both `python -m` and direct execution. Keep that bootstrap.

## 4. Write `manifest.yaml`

Per-parliament metadata that [OpenParliamentTV-Conductor](https://github.com/OpenParliamentTV/OpenParliamentTV-Conductor) reads to know what stages this parliament supports and where to find its data. See [optv/parliaments/DE/manifest.yaml](../optv/parliaments/DE/manifest.yaml):

```yaml
name: "Deutscher Bundestag"
language: deu                   # ISO 639-3
periods: [17, 18, 19, 20, 21]   # legislative terms covered
supported_stages: [download, parse, merge, nel, align, ner]
entity_dump_url: "https://de.openparliament.tv/data/entity-dump/?type=all&wiki=true&exclude_document=true"
default_retry_count: 20
default_retry_delay_max: 10
```

`supported_stages` lets parliaments opt out of stages that don't apply (e.g. a parliament with pre-aligned source data can omit `align`).

## 5. Implement Stage 1 (parliament-specific)

These three packages are where most of the parliament-specific work lives:

- **`scraper/`** — Download proceedings and media into `<data_dir>/original/{proceedings,media}/`. Handle pagination, rate limits, and transient failures (the DE implementation uses `--retry-count` because the Bundestag media server returns frequent 503s). Idempotent: only fetch what's missing.
- **`parsers/`** — Convert the parliament's native format into intermediate per-session JSON. Two streams (proceedings and media) are kept separate at this point because they often need different cleanup logic.
- **`merger/`** — Join the two streams into Stage 2 JSON, one record per speech. The DE merger uses Needleman-Wunsch alignment to match transcript speech entries against media items; new parliaments can use whatever join logic the source data permits.

The merger's output must validate against [stage2-full.schema.json](../optv/shared/schema/stage2-full.schema.json). Run the validator early and often:

```bash
python -m optv.shared.validators.cli --file <data_dir>/cache/merged/<session>-merged.json --schema full
```

## 6. Configure paths

`common.py` exposes a `Config` class that defines the on-disk layout:

```
<data_dir>/
├── original/{media,proceedings}/   # raw downloads
├── cache/{merged,aligned,ner}/     # per-stage outputs
├── processed/                      # published Stage 2 files
└── metadata/                       # NEL entity dumps
```

Stage outputs go into `cache/`; `processed/` holds what the platform actually consumes. The `is_newer()` and `status()` helpers drive the mtime-based "only run if input is newer than output" behaviour. If the parliament needs additional directories, extend the `Config` class — don't hardcode paths in stage scripts.

## 7. Wire the workflow

`workflow.py` is the main entry point. Each `--*` flag enables one stage:

```bash
./optv/parliaments/<CODE>/workflow.py --period=N <data_dir> \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

The shared stages (NEL, alignment, NER) are imported from `optv.shared.*` and don't need re-implementing — see how the DE workflow calls them. The publish step copies the latest valid cache file to `<data_dir>/processed/` and runs schema + semantic validation.

## 8. External assets

- **Wikidata entity dump** — JSON file mapping known speaker names + electoral periods to Wikidata QIDs. URL goes in `manifest.yaml` as `entity_dump_url`. The NEL stage reads from this.
- **Entity-fishing API endpoint** — required by the NER stage. Pass via `--ner-api-endpoint` or set in `manifest.yaml`. Public instances exist; for production, run your own.
- **`ffmpeg` and `espeak`** — required by the alignment stage (aeneas dependency).
- **A spaCy model for the parliament's language** — `de_core_news_md` for German. Install via `python -m spacy download <model>`.

## 9. Verify end-to-end

1. Run the full workflow on one or two sessions with `--limit-session <regex>`.
2. Inspect the published file in `<data_dir>/processed/`.
3. Run the validator over the whole processed directory:

   ```bash
   python -m optv.shared.validators.cli --dir <data_dir>/processed --schema full
   ```

4. Spot-check sentence timings, Wikidata IDs on a known speaker, and entity extraction quality.

There is no automated test suite — validation output is the primary signal.

## 10. Onboard with Conductor

Once the parliament works standalone, add it to the Conductor's `config/parliaments.yaml` so it appears in the web UI. See the [Conductor README](https://github.com/OpenParliamentTV/OpenParliamentTV-Conductor) for the deployment configuration format.
