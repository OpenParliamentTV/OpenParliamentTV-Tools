# Legislative Yuan (TW)

Parser/merger for the Republic of China's Legislative Yuan (立法院), term 11 (2024–). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Video and the machine transcript come from the third-party aggregator [ly.govapi.tw/v2](https://ly.govapi.tw/v2/) (operated by [openfunltd](https://github.com/openfunltd), a Taiwanese civic-tech company; also the source backing [`billy3321/ivod_transcript_db`](https://github.com/billy3321/ivod_transcript_db)). The aggregator wraps the official `data.ly.gov.tw` open-data and IVOD services, adds per-speech AI transcripts ([WhisperX](https://github.com/m-bain/whisperX)) + speaker diarization (pyannote), and exposes filtering by term/session/meeting code/date. Speech kinds and the official text come from the Legislative Yuan's own services. **Media is the authoritative spine**.

- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): `GET /v2/ivods?會議資料.會議代碼={code}` returns one record per IVOD clip with `IVOD_ID`, HLS URL, exact start/end timestamps, speaker name (`委員名稱`), duration, and meeting metadata. Lands in `original/media/{session}-ivods.json` (raw) → `original/media/{session}-media.json` (parsed). Full-meeting videos (`影片種類="Full"`) are filtered out so only per-speech clips survive. A plenary meeting code can span several sitting days (e.g. `院會-11-6-5` sat on two days), so the index is re-fetched when a new sitting day of the meeting appears.
- **Speech kinds** ([`scraper/fetch_speech_kinds.py`](scraper/fetch_speech_kinds.py)): open data dataset 221 (`https://data.ly.gov.tw/odw/ID221Action.action?meetingDateS=…&meetingDateE=…&fileType=json`, ROC-calendar dates) lists each legislator's speech kind per sitting day (`國是論壇`, `法案發言`, `質詢`, `政黨質詢`, `個人質詢`, `詢問`, …). The spoken rows of the meeting land in `original/media/{session}-speechkinds.json`. The rows carry no time or `IVOD_ID`, so the media parser pairs them with clips by (sitting date, legislator); a legislator who speaks twice on a day gets their rows in order of speaking.
- **Machine transcript** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): `GET /v2/ivods/{IVOD_ID}` per clip — same fields plus `transcript.whisperx` (segment-level timings + text) and `transcript.pyannote` (speaker turns). Raw lands in `original/proceedings/{session}-details.json` (fetched incrementally: only clips not yet in the file, or whose WhisperX transcript was not published yet). Each WhisperX segment becomes one sentence, with its timings, of the `textContents[]` entry `type: "generated"`.
- **Official text** ([`scraper/fetch_records.py`](scraper/fetch_records.py) → [`parsers/speech_record.py`](parsers/speech_record.py)): `https://ivod.ly.gov.tw/Demand/Speech/{IVOD_ID}` (the `speechRecordUrl` of open data dataset 421) returns the gazette record of a clip as an HTML fragment. It is empty until the gazette is published, weeks after the sitting; clips without text are re-checked about once a day for up to 180 days after the sitting. Filled pages land in `original/proceedings/{session}-records.json`. The parser splits the fragment into speaker turns (`陳委員昭姿：`, `卓院長榮泰：`, `主席：`, `伍麗華 Saidhai Tahovecahe 委員：`), one `textBody` item each, with `speakerstatus` `main-speaker` for the clip's legislator, `president` for the chair and `speaker` for officials. It becomes the `textContents[]` entry `type: "proceedings"`, listed first. Its sentences are untimed.
- Both proceedings-side streams are parsed into `original/proceedings/{session}-proceedings.json`.
- **No `align_prep.py`**: HLS per-speech clips are addressable directly via `media.videoFileURI`, so no audio slicing is needed.

The Legislative Yuan's hosts (`data.ly.gov.tw`, `ivod.ly.gov.tw`) only complete a TLS handshake with legacy renegotiation, which [`scraper/ly_api.py`](scraper/ly_api.py) enables for those requests (`legacy_tls=True`). ly.govapi.tw answers bursts with `429 Too Many Requests`; the client paces at one request per ~2 s and retries 429 and 5xx responses with backoff.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) joins on the **string `IVOD_ID`** (each per-speech IVOD has one; both streams carry it verbatim). 1:1 left join from media → proceedings. Media-only speeches surface as `debug.merge.text-missing = true`. Proceedings-only entries can't happen by construction (proceedings are fetched per IVOD listed in media) but the merger guards against them anyway.

The merger builds `agendaItem` from the media record: the clip's speech kind as `title`/`officialTitle`, with `type`/`nativeType` from [`classify_tw`](../../shared/agenda_types.py) (e.g. `政黨質詢` → `questioning_of_the_government` / `TW-party_interpellation`). A clip without a dataset-221 match keeps the sitting title with `nativeType: "TW-plenary"`.

## Alignment (whisperx, not aeneas)

[`workflow.py:_align`](workflow.py) skips aeneas/espeak entirely and reads the WhisperX segments back out of the raw IVOD detail bundle into the `generated` entry: each segment becomes one `sentence` with `timeStart`/`timeEnd` already populated (formatted as the numeric strings the Stage 2 schema requires). The official `proceedings` entry stays untimed. Speeches with no WhisperX data pass through unaligned. `debug.alignSource: "whisperx"` records the provenance and `debug.alignDuration` carries the last segment's `end` for the shared status detector.

aeneas with espeak's `cmn` voice produces poor-quality Mandarin alignment, and the API already gives us higher-quality Whisper output for free — there is no point in running aeneas as well.

## Running

```bash
# One plenary (drop --session to discover the last 14 days):
./optv/parliaments/TW/workflow.py --period=11 <data_dir> \
    --session 院會-11-5-11 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities \
    --ner-api-endpoint http://localhost:8090/service/
```

Without `--session`, `--download-original` discovers plenary meetings with IVODs on any day from `--since YYYY-MM-DD` (default: the last 14 days) to today via `GET /v2/ivods?日期=YYYY-MM-DD`; a backfill passes an early `--since`. `--session <code>` (repeatable) downloads those meetings instead. `--period=11` selects term 11. Session keys are `{屆:02d}{會期:02d}{會次:03d}` (e.g. `1105011` = 院會-11-5-11). `--limit-ivods N` is testing-only.

A run that merges makes two passes. Pass 1 runs the requested stages: download (discovery, speech kinds, details, official text), merge, entity linking, WhisperX timings, entity extraction. When official text arrives for a session that is already published, the publish guard refuses the changed transcript and the merge/align stages skip already-aligned sessions, so the download step queues the session in `cache/tw-official-text-upgrades.txt`; pass 2 re-runs exactly the queued sessions in scope with rebuild semantics and takes them off the queue.

The NEL entity dump is built locally:

```bash
.venv/bin/python -m optv.parliaments.TW.scraper.build_entity_dump <data_dir>
```

This joins a Wikidata SPARQL pull (`P39 wd:Q6310593`, "Member of the Legislative Yuan") with the ly.govapi.tw legislators roster for the current term, plus a hand-curated list of Taiwanese parties. Output: `<data_dir>/metadata/entities.json` (~1100 entities; coverage drops for newer 2024+ term-11 legislators not yet in Wikidata).

## Text provenance and rights

| `textContents[].type` | Source | `creator` | `license` (manifest stream) |
|---|---|---|---|
| `proceedings` | Official gazette record per clip, `ivod.ly.gov.tw/Demand/Speech/{IVOD_ID}` | 立法院 (Legislative Yuan) | [Legislative Yuan open-data declaration](https://data.ly.gov.tw/copyprotection.action) (`proceedings`) |
| `generated` | WhisperX machine transcript by ly.govapi.tw, `ly.govapi.tw/v2/ivods/{IVOD_ID}` | ly.govapi.tw (OpenFun Ltd.) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) (`transcript`) |

The Legislative Yuan's declaration allows free reuse with attribution, except for works it marks as needing consent. The IVOD video is published under [non-commercial-use-only terms](https://ivod.ly.gov.tw/Copyright) (`media.license`), and the WhisperX transcript is derived from that video. Pipeline outputs carry `media.creator` / `media.license` / `textContents[].license` so downstream deployments can act on this; the Tools repo neither enforces nor relaxes the restrictions.

## Known limitations

- **Committee meetings are out of scope.** Only plenary meetings (`會議代碼` `院會-T-SP-MN`) become OPTV sessions. Committee meetings (`委員會-*`) carry rich material but multiply the volume; left as a future extension. Extraordinary-session codes are skipped by discovery.
- **Official text arrives late.** Until the gazette is published, a speech carries only the `generated` transcript.
- **NEL coverage is bounded by Wikidata.** Roughly 1030 LY members have `P39 wd:Q6310593` set on Wikidata. Around 40 current term-11 members do not yet, so they appear in `entities.json` with no QID and the NEL stage leaves their `wid` empty. The Stage 2 validator surfaces this as warnings (`semantic.people.wid.missing`).
- **NER needs `db-zh`.** The shared `entity-fishing` deployment must have the Chinese KB loaded (`sciencialab/entity-fishing-db-zh` on HuggingFace). Without it, the NER stage runs cleanly but every `entities[]` is empty.
- **`gazette` field mostly empty.** ly.govapi.tw exposes a `gazette` field on older IVODs (the official-gazette transcript, paragraph-level); the parser uses it as the `proceedings` entry when the IVOD site has no text, but the field is `null` for most term-11 records.
