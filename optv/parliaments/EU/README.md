# European Parliament (EU)

Parser/merger for the European Parliament plenary (term 10, 2024–2029). Built around the official European Parliament Open Data Portal API (`data.europarl.europa.eu/api/v2`) and the glcloud HLS media stream. Verbatim text is pulled in English; per-speech NER and audio alignment run as a single English pipeline. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, joined per plenary day. The proceedings stream carries per-speech text plus EP-supplied timing; the media side resolves those timestamps to a per-sitting HLS audio track. Session key is the plenary day (`YYYYMMDD`).

- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): per plenary day we pull `GET /meetings/{id}` for the day envelope (+ its `consists_of` agenda items), plus `GET /speeches?sitting-date=YYYY-MM-DD&sitting-date-end=YYYY-MM-DD&include-output=xml_fragment&limit=100` for all speeches with the verbatim `xml_fragment` payloads inline (~6 paginated calls per 459-speech day). The English `xml_fragment` gives us structured `<person refersTo="epdata:person/NNN">`, `<organization>` (faction abbr), `<blockContainer>/<p>` paragraphs, and millisecond-precision `<speech startTime endTime>`. The HTTP client lives in [`scraper/ep_api.py`](scraper/ep_api.py).
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): the glcloud content-manager SSR page (`control.eup.glcloud.eu/content-manager/content-page/{event-ref}`). The HLS master URL plus the per-rendition language tags (`LANGUAGE="qaj"` = OR/floor) are extracted from the `<script id="ng-state">` JSON blob. [`align_prep.py`](align_prep.py) downloads the floor track and slices per-speech MP3s with ffmpeg for aeneas.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) does a **timestamp-window join**: each speech's UTC `dateStart` (TZ-aware from the API) is matched against the sitting windows in the media JSON (`sittingStart`..`sittingEnd`). Multi-sitting days fold cleanly under one session — each speech resolves its own per-sitting audio URL.

NEL ([`optv/shared/nel.py`](../../shared/nel.py)) prefers a direct `epId → entities.json` lookup before the cleaned-label fallback. Because the API gives us a person ID for every MEP, every catalogued MEP gets a guaranteed Wikidata QID match.

## Running

```bash
./optv/parliaments/EU/update <data_dir>
# or, with an explicit day:
./optv/parliaments/EU/workflow.py --period=10 <data_dir> \
    --session 2025-10-08 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
# or for a full calendar year:
./optv/parliaments/EU/workflow.py --period=10 <data_dir> \
    --year 2025 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

The `update` wrapper bakes in `--period=10 --retry-count=10`. Date selection priority: `--session YYYY-MM-DD` (repeatable) > `--year YYYY` (auto-enumerates plenary sittings via `/meetings?year=YYYY`) > `--limit-session YYYYMMDD`. The download stage is a no-op if none of these are provided.

## Access notes

- **No scraping.** The API is plain HTTPS, requires a `User-Agent` header, and is rate-limited to 500 requests / 5 min per endpoint. A 459-speech day needs ~6 paginated calls + 1 meeting + a handful of agenda fetches — well below the cap. The client in [`scraper/ep_api.py`](scraper/ep_api.py) paces calls at ~0.7s with a burst budget and caches responses under `<data_dir>/cache/ep-api/`.
- **ffmpeg ≥7 is required.** Older ffmpegs (e.g. macOS Intel-brew 3.x) lack HTTPS and silently fail to pull HLS from the EP CDN. `brew install ffmpeg` installs 8.x.
- **yt-dlp does not match the EP VOD URLs.** Its `EuroParlWebstream` extractor only matches `multimedia.europarl.europa.eu/.../webstream...` URLs. The scraper feeds the HLS master directly to ffmpeg.

## Known limitations

- **English-only output.** The pipeline takes the API's English translation of each speech (the EP publishes all 24 EU languages alongside the original). NER, NEL, and aeneas alignment all run as a single English pipeline.
- **Entity dump is built offline.** `entity_dump_url` is empty until an EU OPTV platform hosts one. Run `python -m optv.parliaments.EU.scraper.build_entity_dump <data_dir>` once (or when the MEP roster changes) — output goes to `<data_dir>/metadata/entities.json`. Term-10 MEP coverage is ~100% (741/741 verified 2026-05-25) via `data.europarl.europa.eu/api/v2/meps` × Wikidata SPARQL P1186. Non-MEP guests (Commission, Council) are the typical NEL gap.
- **Faction label sync.** The merger emits `faction.label` without the "Group" suffix Wikidata uses canonically. Both forms live in the entity dump's `labelAlternative` — keep `EU_FACTION_QIDS` in [`scraper/build_entity_dump.py`](scraper/build_entity_dump.py) synced with `EU_FACTION_LABELS` in [`parsers/common.py`](parsers/common.py).
- **Translation latency.** The API publishes English translations within ~24h of a sitting. For very recent days, an `en` `xml_fragment` may be missing on individual speeches — the parser falls back to `fr → de → es → it → first-available` and records the fallback in `debug.fallbackLang`.
- **Bulk-list omissions.** `GET /speeches?sitting-date=…` reports the day's full `meta.total` but occasionally omits individual speeches from the paginated listing even though they exist (verified 2026-05-26: a real `PLENARY_DEBATE_SPEECH` accessible via `GET /speeches/{id}` and via `?person-id=…` was absent from the bulk listing for the same date). Typical gap is small (~7% of the day's debate speeches). No code-side fix exists; treat the bulk-list as the canonical set per the API contract.
- **Native-language speaker labels.** The EN `xml_fragment` preserves the original-language `<person>` label for chair-of-the-sitting speeches (e.g. "Die Präsidentin" stays German even in the EN payload). The text body is English, the `epId` is correct, and NEL still resolves the speaker via the epId index — only the display label is non-English.
- **Scope.** Term 10 plenary only. Older terms should largely re-use this code; the API serves earlier terms too but with potentially different MEP catalogues and faction QIDs.
