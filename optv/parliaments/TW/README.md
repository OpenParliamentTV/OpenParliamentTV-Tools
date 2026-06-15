# Legislative Yuan (TW)

Parser/merger for the Republic of China's Legislative Yuan (立法院), term 11 (2024–). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, both pulled from the same third-party aggregator [ly.govapi.tw/v2](https://ly.govapi.tw/v2/) (operated by [openfunltd](https://github.com/openfunltd), a Taiwanese civic-tech NPO; also the source backing [`billy3321/ivod_transcript_db`](https://github.com/billy3321/ivod_transcript_db)). The aggregator wraps the official `data.ly.gov.tw` open-data and IVOD services, adds per-speech AI transcripts ([WhisperX](https://github.com/m-bain/whisperX)) + speaker diarization (pyannote), and exposes filtering by term/session/meeting code that the upstream open-data portal does not (the `data.ly.gov.tw` `selectTerm` filter is non-functional for term 11 — verified 2026-05-27). **Media is the authoritative spine**.

- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): `GET /v2/ivods?會議資料.會議代碼={code}` returns one record per IVOD clip with `IVOD_ID`, HLS URL, exact start/end timestamps, speaker name (`委員名稱`), duration, and meeting metadata. Lands in `original/media/{session}-ivods.json` (raw) → `original/media/{session}-media.json` (parsed). Full-meeting videos (`影片種類="Full"`) are filtered out so only per-speech clips survive.
- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): `GET /v2/ivods/{IVOD_ID}` per clip — same fields plus `transcript.whisperx` (segment-level timings + text) and `transcript.pyannote` (speaker turns). Raw lands in `original/proceedings/{session}-details.json`; parsed in `original/proceedings/{session}-proceedings.json`. Each whisperx segment becomes one Stage 2 sentence (timings already aligned by Whisper).
- **No `align_prep.py`**: HLS per-speech clips are addressable directly via `media.videoFileURI`, so no audio slicing is needed.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) joins on the **string `IVOD_ID`** (each per-speech IVOD has one; both streams carry it verbatim). 1:1 left join from media → proceedings. Media-only speeches surface as `debug.merge.text-missing = true`. Proceedings-only entries can't happen by construction (proceedings are fetched per IVOD listed in media) but the merger guards against them anyway.

## Alignment (whisperx, not aeneas)

[`workflow.py:_align`](workflow.py) skips aeneas/espeak entirely and reads the WhisperX segments back out of the raw IVOD detail bundle. Each segment becomes one `sentence` with `timeStart`/`timeEnd` already populated (formatted as the numeric strings the Stage 2 schema requires). Speeches with no whisperx data pass through unaligned. `debug.alignSource: "whisperx"` records the provenance and `debug.alignDuration` carries the last segment's `end` for the shared status detector.

aeneas with espeak's `cmn` voice produces poor-quality Mandarin alignment, and the API already gives us higher-quality Whisper output for free — there is no point in running aeneas as well.

## Running

`--session <code>` (repeatable) is required for downloads because the LY API has no "current plenary" auto-discovery endpoint. The `update` wrapper and `Makefile` cover **post-download** stages only — merge + NEL + align + NER over whatever raw files already exist under `original/`:

```bash
./optv/parliaments/TW/update <data_dir>
# or, with finer control / to download a new plenary:
./optv/parliaments/TW/workflow.py --period=11 <data_dir> \
    --session 院會-11-5-11 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities \
    --ner-api-endpoint http://localhost:8090/service/
```

`--period=11` selects term 11. Session keys are `{屆:02d}{會期:02d}{會次:03d}` (e.g. `1105011` = 院會-11-5-11). To enumerate every plenary in a session-period for backfill, see [`scraper/ly_api.py:LYApiClient.list_plenary_meeting_codes`](scraper/ly_api.py); `--limit-ivods N` is testing-only.

The NEL entity dump is built locally:

```bash
.venv/bin/python -m optv.parliaments.TW.scraper.build_entity_dump <data_dir>
```

This joins a Wikidata SPARQL pull (`P39 wd:Q6310593`, "Member of the Legislative Yuan") with the ly.govapi.tw legislators roster for the current term, plus a hand-curated list of Taiwanese parties. Output: `<data_dir>/metadata/entities.json` (~1100 entities; coverage drops for newer 2024+ term-11 legislators not yet in Wikidata).

## Known limitations

- **`--session` required for downloads.** The LY API has no auto-discovery endpoint for "the latest plenary"; the wrapper can't bake one in. Backfill across many meetings: enumerate via `LYApiClient.list_plenary_meeting_codes(term, session_period)` and pass each code on the command line.
- **Committee meetings are out of scope.** Only plenary meetings (`會議代碼` starting with `院會-`) become OPTV sessions. Committee meetings (`委員會-*`) carry rich material but multiply the volume; left as a future extension.
- **NEL coverage is bounded by Wikidata.** Roughly 1030 LY members have `P39 wd:Q6310593` set on Wikidata. Around 40 current term-11 members do not yet, so they appear in `entities.json` with no QID and the NEL stage leaves their `wid` empty. The Stage 2 validator surfaces this as warnings (`semantic.people.wid.missing`).
- **NER needs `db-zh`.** The shared `entity-fishing` deployment must have the Chinese KB loaded (`sciencialab/entity-fishing-db-zh` on HuggingFace). Without it, the NER stage runs cleanly but every `entities[]` is empty.
- **Video copyright.** The IVOD service publishes under [non-commercial-use-only terms](https://ivod.ly.gov.tw/Copyright); proceedings text is [Open Government Data Licensed](https://data.gov.tw/license) (CC BY 4.0 compatible). Pipeline outputs include `media.creator` / `media.license` / `textContents[].license` so downstream deployments can act on this; the Tools repo neither enforces nor relaxes the restrictions.
- **`gazette` transcripts mostly empty for recent IVODs.** ly.govapi.tw exposes a `gazette` field on older IVODs (the official-gazette transcript, paragraph-level) but the field is `null` for most term-11 records as of mid-2026. Whisperx is the primary text path.
