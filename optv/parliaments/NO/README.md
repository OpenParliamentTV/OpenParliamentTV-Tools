# Stortinget (NO)

Parser/merger for the Norwegian Stortinget, terms 2017-2021, 2021-2025 and 2025-2029 (OPTV-internal periods `20`, `21`, `22`). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, both fetched per meeting. **Proceedings is the spine** — the publikasjon XML carries every speech in document order with the speaker's `personID` and a `[HH:MM:SS]` clock anchor; media metadata only contributes per-part video URLs and UTC anchors.

- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): `eksport/publikasjon?publikasjonid=refs-YYYYYY-MM-DD` returns full Referat XML. The parser walks `Startseksjon` (one procedural item) and every child of `Hovedseksjon/Saker` as one agenda item: `Sak`, `Sporretime` (question time), `Interpellasjon`, `Sakreferat` and the voting block (`VoteringerStart`; `Voteringer` in 2016-2017). The agenda number is `saksKartNr`, or the "Sak nr. N" in `Sakshode/Saknr` where the attribute is missing; a voting block becomes `votering-{k}`. Speech elements are `<Hovedinnlegg>` / `<Replikk>` / `<Presinnlegg>`; their text is every `<A>` in document order, including quotations (`Sitat`) and list items (`Liste`), without tables and footnotes. Sentence segmentation via spaCy `nb_core_news_md`; lands in `original/proceedings/{session}-proceedings.json`.
- **Elements without a clock time** (most president's remarks in voting blocks, and most president's remarks in Referats before 2025-2026; `[00:00:00]` is a placeholder) are not emitted as records. Their text is appended as a further `textBody` block (`speakerstatus` `president`/`vice-president`, or `speaker` for a member) to the preceding record, whose clip spans the time they were spoken in; `debug.foldedElements` counts them. A member speaking that way is added to `people[]` with context `speaker`.
- **Text ids**: `textContents[].originTextID` is `{referat_id}-{HH:MM:SS}`, the form Qbrick events use as `reference_id` (with a `-2` … suffix when two elements share a second). The element `Id` is no stable key: a Referat gets new `Id`s with every version until it is final (`Komplett`), other Referats reuse the same values, and most president's remarks from 2025-2026 have none; it is used (as `{referat_id}-{Id}`) only for meetings without clock times.
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): two unauth GETs per video part. The archive HTML page carries `"qbrickAccountId":"…"` and `"qbrickVideoId":"…"` for each `del`; the Qbrick public API at `https://video.qbrick.com/api/v1/public/accounts/{account}/medias/{qbvid}` returns MP4 renditions + HLS + `custom.TC_in` (UTC start) and, for most parts from 2022-2023 on, a chapter track. Stored as a parts list (with the cleaned, start-sorted chapters) in `original/media/{session}-media.json`.
- **Per-speech audio** ([`align_prep.py`](align_prep.py)): per-part MP4 is converted to MP3 once via ffmpeg, then sliced `[startOffset, startOffset+duration]` per speech (keyframe-aligned `-c copy` — aeneas refines boundaries).

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) reads each speech's `dateStart` (Europe/Oslo with offset, from `[HH:MM:SS]`), converts it to UTC and matches it against the part whose `[tc_in_utc, tc_in_utc + duration]` window contains it. `startOffset = speech_utc - tc_in_utc`. The end comes from the part's Qbrick chapter that starts at the speech (±1 s) where one exists, else from the next speech's start, else from the end of the part; `debug.merge.durationSource` records which. Chapters end where the next one begins, so they change the result only for the last speech of a part. Per-speech URLs are Media Fragment URIs (`#t=start,end`) on the part MP4; `sourcePage` opens the archive player at the speech with `msid={start}` (the player ignores `t`). `dateStart`, `dateEnd` and `meta.dateStart`/`meta.dateEnd` carry the Europe/Oslo offset; the meta range runs from the first speech's start to the last speech's end.

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

- `--period` is the OPTV-internal term index: `20` = 2017-2021, `21` = 2021-2025, `22` = 2025-2029 (string ids in the API; see `common.TERM_TO_PERIOD`). `update` and the Makefile run `--period=22`.
- `--session 2025-2026` limits the sesjon-year(s) within the period (default: all sesjons of the period).
- `--meid 11518` restricts work to specific meeting IDs.
- Session keys are `{period}_{moteid}` (e.g. `22_11518`).

## Downloads and rate limit

data.stortinget.no allows 100 API calls per minute and answers above that with HTTP 429. All NO fetches go through [`scraper/http_get.py`](scraper/http_get.py), which spaces calls to data.stortinget.no at least 0.7 s apart and retries 429 (after `Retry-After`, or a minute), 5xx and network errors.

- The meetings overview of a sesjon is refetched on every run until the sesjon has ended (1 October of its end year), so new meetings and late Referat ids are picked up; it is rewritten only when the meeting list changed.
- A Referat passes through the states `Arbeid` → `Kvalitetssikret` → `Komplett` (`Forhandlinger/@Status`). Only a `Komplett` Referat is served from the cache; any other is refetched on every run and rewritten only when its bytes changed.

## Known limitations

- **Entity-fishing has no Norwegian KB — using Swedish as a cross-lingual proxy.** Entity-fishing `0.0.6` ships configs for 19 languages (ar/bn/de/en/es/fa/fr/hi/it/ja/nl/pt/ru/sv/uk/zh and a few others); `no` is not one. The pipeline therefore sets `manifest.locale.entityfishing_language: sv` and detects entities with Norwegian spaCy (`nb_core_news_md`) but disambiguates against the Swedish KB. This works well in practice — the underlying Wikidata KB is language-agnostic, and Norwegian + Swedish share most proper-noun surface forms. The Trontaledebatten sample yields ~1 200 linked entities across 160/175 speeches with correct Norwegian Q-IDs (Arbeiderpartiet → Q190219, Stortinget → Q109016, Oslo → Q585, Norge → Q20, …). A native `db-no` would marginally improve coverage on Norwegian-specific terms but requires building from scratch with [GRISP](https://github.com/kermitt2/grisp).

- **Entity dump is local-only.** `manifest.yaml`'s `entity_dump_url` is empty until `no.openparliament.tv` is set up; the operator runs [`scraper/build_entity_dump.py`](scraper/build_entity_dump.py) once, which writes `<data_dir>/metadata/entities.json` from Wikidata: every holder of `P39=Q9045502` ("member of the Parliament of Norway") or of `P3072` (Storting person ID), plus a hand-curated party list. The Stortinget person ID is stored as `additionalInformation.originPersonID`, so NEL links speakers by their `personID` first and falls back to the cleaned label.
- **Terms before 2017.** Referats follow the current `forhandlinger.dtd` from session 2016-2017 on; the terms from 2017-2021 are wired. Earlier Referats use `forhandl_xml.dtd` (lowercase elements, no `personID`, no speech `Id`) and are not implemented.
- **Meetings without clock times.** A few Referats (e.g. the constituent and opening meetings) carry no `[HH:MM:SS]` on any speech element. Their speeches are emitted without `dateStart`, and the merger gives them an empty media block (`debug.merge.media-missing`).
- **Norway uses two written languages.** All speeches are emitted as `language="nb"` regardless of whether the original used Bokmål (`nb`) or Nynorsk (`nn`). spaCy `nb_core_news_md` segments both adequately, but Nynorsk-aware downstream processing is a future improvement.
- **`mote_dato_tid` is scheduled start, not video t=0.** Always use Qbrick `custom.TC_in` for offset arithmetic; the meeting overview's `mote_dato_tid` is human-targeted and can drift.
- **Multi-part meetings.** Long sessions split into `del=1`, `del=2`, … each with its own `qbvid` and `TC_in`. The merger assigns each speech to the first part whose window contains its clock time (parts can overlap); speeches outside every part window (between two parts, or after the last part where the archive has no video for the rest of the meeting) get a `debug.merge.media-missing` marker and an empty media block.
- **Qbrick timecodes are taken as given**, except that a `TC_in` on an earlier day than the part's own `custom.Dato` (a one-day typo in the record) is moved to that date together with `TC_out` ([`scraper/qbrick.py`](scraper/qbrick.py) logs a warning).
- **Qbrick chapters are incomplete.** Some parts have an empty chapter track or chapters for only part of the speeches, so the merger uses them only as a refinement of the clock-time offsets.
- **No live scraper for ongoing sessions.** Nett-TV / HLS-live is archive-only here; live coverage is out of scope.
