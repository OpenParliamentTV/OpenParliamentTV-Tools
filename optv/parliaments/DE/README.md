# German Bundestag (DE)

Parliament-specific Stage 1 implementation for the German Bundestag. For pipeline concepts and the Stage 2 format, see [Architecture/PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md) and [Architecture/STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md). For the high-level repo overview and quick start, see the [Tools README](../../../README.md).

## Layout

- [`scraper/`](scraper/) — Fetch proceedings (TEI XML) and media (RSS feeds) from the Bundestag's open-data sources.
- [`parsers/`](parsers/) — Convert TEI XML and media RSS into intermediate per-session JSON.
- [`merger/`](merger/) — Join media + proceedings into Stage 2 JSON. Uses Needleman-Wunsch alignment to match speeches across the two streams.
- [`workflow.py`](workflow.py) — Main entry point. A thin wrapper that defines DE-specific download/parse/merge/align adapters (`WorkflowHooks`) and calls the shared orchestrator in [`optv/shared/workflow.py`](../../shared/workflow.py). Each `--*` flag enables one stage; flags are idempotent; `--force` re-runs.
- [`common.py`](common.py) — `Config` class (paths, file naming, mtime checks). Re-exports `SessionStatus` and the publish helpers (`data_signature`, `is_demotion`, `carry_forward_wids`, `carry_forward_enrichments`) from [`optv/shared/`](../../shared/).
- [`manifest.yaml`](manifest.yaml) — Per-parliament metadata read by Conductor (supported stages, periods, entity dump URL, retry defaults).
- [`update`](update) — Shell wrapper baking in `--period=21 --retry-count=20` for routine runs.
- [`Makefile`](Makefile) — `make download` / `make all` for fine-grained, mtime-driven invocations.

## Running

```bash
./workflow.py --period=21 <data_dir>
```

`<data_dir>` should be a clone of [OpenParliamentTV-Data-DE](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE). Stage flags: `--download-original`, `--merge-speeches`, `--link-entities`, `--align-sentences`, `--extract-entities`. A lockfile (`<data_dir>/optv.lock`) blocks concurrent runs.

The Bundestag media server frequently returns 503s when building responses for older periods. `--retry-count` retries with random backoff (≤10s) between attempts; the scraper only re-downloads files that aren't already present.

## Dependencies

`pip install -r ../../../requirements.txt`. `aeneas` (alignment) needs `ffmpeg` + `espeak`. NER needs the `de_core_news_md` spaCy model and an `entityfishing` API endpoint.

## Adding another parliament

See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) — this directory is the reference template.
