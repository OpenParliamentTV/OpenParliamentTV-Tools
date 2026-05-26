# Deutscher Bundestag (DE)

Parliament-specific Stage 1 pipeline for the German Bundestag — the reference implementation other parliaments follow. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md); for the wider system see [Architecture/PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md) and [Architecture/STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md).

## Data model

Two streams, both fetched from public Bundestag sources, joined per session. Proceedings are the authoritative spine — every published speech keeps its transcript text.

- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): the official TEI XML Plenarprotokoll. One file per session lands in `original/proceedings/`, parsed to intermediate JSON.
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py), [`scraper/update_media.py`](scraper/update_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): the Bundestag media RSS feed (one per period). Per-speech MP4 URLs and metadata land in `original/media/`.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) runs **Needleman-Wunsch alignment** on the two streams, matching transcript speech entries against media items by speaker and agenda position. Confidence is recorded in `debug.confidence`.

## Running

```bash
./optv/parliaments/DE/update <data_dir>
# or, for finer control:
./optv/parliaments/DE/workflow.py --period=21 <data_dir> \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

`<data_dir>` should be a clone of [OpenParliamentTV-Data-DE](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-DE). The `update` wrapper bakes in `--period=21 --retry-count=20`. Each `--*` stage flag is opt-in and idempotent; `--force` re-runs an already-completed stage. A lockfile (`<data_dir>/optv.lock`) blocks concurrent runs. `make download` / `make all` mirror the mtime-driven flow.

## Known limitations

- **Media-server 503s on older periods.** The Bundestag media server frequently 503s when building responses for archived periods. `--retry-count` (default 20 via the `update` wrapper) retries with random backoff (≤10s); only missing files are re-fetched.
- **NER/NEL externals required.** Alignment needs `ffmpeg` + `espeak`; NER needs the `de_core_news_md` spaCy model and an `entityfishing` API endpoint (`--ner-api-endpoint`).
