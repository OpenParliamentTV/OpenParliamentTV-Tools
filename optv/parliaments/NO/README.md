# Stortinget (NO)

Parser/merger for the Norwegian Stortinget (current period 2025-2029, OPTV-internal index `22`). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, both fetched per meeting. **Proceedings is the spine** — the publikasjon XML carries every speech in document order with the speaker's `personID` and a `[HH:MM:SS]` clock anchor; media metadata only contributes per-part video URLs and UTC anchors.

- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): `eksport/publikasjon?publikasjonid=refs-YYYYYY-MM-DD` returns full Referat XML (`<Hovedinnlegg>` / `<Replikk>` / `<Presinnlegg>` under `<Sak saksKartNr>`). Sentence segmentation via spaCy `nb_core_news_md`; lands in `original/proceedings/{session}-proceedings.json`.
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): two unauth GETs per video part. The archive HTML page carries `"qbrickVideoId":"…"` for each `del`; the Qbrick public API at `https://video.qbrick.com/api/v1/public/accounts/{account}/medias/{qbvid}` returns MP4 renditions + HLS + `custom.TC_in` (UTC start). Stored as a parts list in `original/media/{session}-media.json`.
- **Per-speech audio** ([`align_prep.py`](align_prep.py)): per-part MP4 is converted to MP3 once via ffmpeg, then sliced `[startOffset, startOffset+duration]` per speech (keyframe-aligned `-c copy` — aeneas refines boundaries).

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) walks the per-speech proceedings list, interprets each `dateStart` (naive Europe/Oslo, from `[HH:MM:SS]`) as UTC, and matches each speech against the part whose `[tc_in_utc, tc_in_utc + duration]` window contains it. `startOffset = speech_utc - tc_in_utc`; duration is derived from the next speech's start, clamped to the part's tail. Per-speech URLs are Media Fragment URIs (`#t=start,end`) on the part MP4.

## Running

Stortinget's open data API has no auth and the per-sesjon overview is small, so download is part of the default pipeline:

```bash
# Wikidata-derived entity dump (no.openparliament.tv doesn't host one yet)
python3 -m optv.parliaments.NO.scraper.build_entity_dump <data_dir> --period 22

# Full pipeline
./optv/parliaments/NO/update <data_dir>
# or, with finer control:
./optv/parliaments/NO/workflow.py --period=22 <data_dir> \
    --meid 11518 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities \
    --ner-api-endpoint http://localhost:8090/service/
```

- `--period=22` corresponds to Storting 2025-2029 (string id `"2025-2029"` in the API; see `common.TERM_TO_PERIOD`).
- `--session 2025-2026` limits the sesjon-year(s) within the period (default: all sesjons of the period).
- `--meid 11518` restricts work to specific meeting IDs.
- Session keys are `{period}_{moteid}` (e.g. `22_11518`).

## Known limitations

- **Entity-fishing has no Norwegian KB — using Swedish as a cross-lingual proxy.** Entity-fishing `0.0.6` ships configs for 19 languages (ar/bn/de/en/es/fa/fr/hi/it/ja/nl/pt/ru/sv/uk/zh and a few others); `no` is not one. The pipeline therefore sets `manifest.locale.entityfishing_language: sv` and detects entities with Norwegian spaCy (`nb_core_news_md`) but disambiguates against the Swedish KB. This works well in practice — the underlying Wikidata KB is language-agnostic, and Norwegian + Swedish share most proper-noun surface forms. The Trontaledebatten sample yields ~1 200 linked entities across 160/175 speeches with correct Norwegian Q-IDs (Arbeiderpartiet → Q190219, Stortinget → Q109016, Oslo → Q585, Norge → Q20, …). A native `db-no` would marginally improve coverage on Norwegian-specific terms but requires building from scratch with [GRISP](https://github.com/kermitt2/grisp).

- **Entity dump is local-only.** `manifest.yaml`'s `entity_dump_url` is empty until `no.openparliament.tv` is set up; the operator runs [`scraper/build_entity_dump.py`](scraper/build_entity_dump.py) once, which writes `<data_dir>/metadata/entities.json` from Wikidata `P39=Q9045502` ("member of the Parliament of Norway") plus a hand-curated party list. There is no `personID`-based fallback yet — match is on cleaned label.
- **Single Stortingsperiode wired.** `manifest.periods=[22]` and `common.TERM_TO_PERIOD` lists only `"2025-2029"`. Previous terms had different XML schemas in the Referat and are not implemented.
- **Norway uses two written languages.** All speeches are emitted as `language="nb-NO"` regardless of whether the original used Bokmål or Nynorsk. spaCy `nb_core_news_md` segments both adequately, but Nynorsk-aware downstream processing is a future improvement.
- **`mote_dato_tid` is scheduled start, not video t=0.** Always use Qbrick `custom.TC_in` for offset arithmetic; the meeting overview's `mote_dato_tid` is human-targeted and can drift.
- **Multi-part meetings.** Long sessions split into `del=1`, `del=2`, … each with its own `qbvid` and `TC_in`. The merger assigns each speech to the correct part by clock-time; speeches outside every part window get a `debug.merge.media-missing` marker and an empty media block.
- **Qbrick account id is constant in code.** `AccrjW9C7ikYk2xPM5xJ4Frag` was identical across every sampled meeting; the resolver does no rediscovery. If Stortinget migrates accounts, [`scraper/qbrick.py`](scraper/qbrick.py) needs an update.
- **No live scraper for ongoing sessions.** Nett-TV / HLS-live is archive-only here; live coverage is out of scope.
