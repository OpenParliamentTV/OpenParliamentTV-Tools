# Assemblée nationale (FR)

Parser/merger for the French National Assembly plenary (legislature 17, 2024–present). Built around the official Assemblée nationale open data (Syceron comptes rendus + AMO acteurs/organes) and the `videos-diffusion.assemblee-nationale.fr` HLS stream. The compte rendu carries the verbatim text **and** the per-speech video offsets, so the merge is a single-source spine; only the séance video URL is resolved externally. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md).

## Data model

Two streams, but the proceedings stream is the **authoritative spine** — it supplies both text and timing. Session key is the Syceron compte-rendu uid suffix (e.g. `2026O1N254`); see [`common.py`](common.py) for the uid ↔ `session.number` encoding (the integer packs year + session-type + séance number so it stays unique across a legislature).

- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): per-séance Syceron compte rendu (`CRSANR5L17S{year}{O|E}{ordre}N{num}.xml`). Targeted fetches use the per-document endpoint `/dyn/opendata/{uid}.xml`; the default bulk mode pulls the legislature's `…/vp/syceronbrut/syseron.xml.zip` and extracts every `CRS*.xml`. The namespaced XML gives `<paragraphe id_acteur=…>` speeches, `<orateur>` speaker labels, the `<point nivpoint=…>` agenda tree, and — once the video is synced — the per-speech `<texte stime="26.68">` offset (seconds into the séance recording). Consecutive paragraphes by the same `id_acteur` within one agenda point are folded into a single speech; `code_style ≠ NORMAL` paragraphes (vote tallies, "(Applaudissements…)") become `comment` text bodies.
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): one HLS video per séance. The compte rendu names its séance in `<seanceRef>RUANR…IDS…</seanceRef>`; the `interventions-video` index pairs that réunion id with a video-compte-rendu id (`data-id="CRV… RU…"`); the CRV page `/dyn/videos/{CRV…}` embeds the HLS master playlist URL. [`align_prep.py`](align_prep.py) downloads that séance track once and slices per-speech MP3s with ffmpeg for aeneas.
- **Entities** ([`scraper/build_entity_dump.py`](scraper/build_entity_dump.py)): AMO10 (`deputes_actifs_mandats_actifs_organes`) ⋈ Wikidata SPARQL (P4123 = AN id) → `metadata/entities.json` (NEL) + `metadata/acteurs.json` (the `id_acteur → groupe` map the proceedings parser needs, since the compte rendu names the speaker but not their group).

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) does a **single-source spine walk** — no cross-source matching is needed because text and timing come from the same document. Speeches are emitted in document order; each speech's `stime` becomes `media.additionalInformation.startOffset`, and `duration` runs to the next speech's offset (median-based fallback for the last speech of the séance). The séance's one HLS URL is attached to every speech as a `#t=start,end` fragment. `sourcePage` is made unique per speech (`…&timeCode=…&i={speechIndex}`) so the semantic validator's duplicate check passes even when two speeches share a `stime` (e.g. a chair hand-over).

NEL ([`optv/shared/nel.py`](../../shared/nel.py)) prefers a direct `id_acteur → entities.json` lookup before the cleaned-label fallback, so every catalogued député gets a guaranteed Wikidata QID.

## Running

```bash
./optv/parliaments/FR/update <data_dir>
# or, with explicit séance keys:
./optv/parliaments/FR/workflow.py --period=17 <data_dir> \
    --session 2026O1N254 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities \
    --ner-api-endpoint http://localhost:8090/service/
```

The `update` wrapper bakes in `--period=17 --retry-count=20` and all stage flags. `--session KEY` (repeatable) targets individual séances via the per-document endpoint; when omitted, the download stage pulls the whole legislature's Syceron bulk archive. `--limit-session <regex>` filters either mode. `--period` is the **legislature**, not a year.

## Access notes

- **ffmpeg ≥ 7 is required** for the align stage. Older builds (e.g. macOS Intel-brew 3.x) are compiled without HTTPS and silently fail to pull the AN HLS stream (`Protocol not found … Did you mean file:https://…`). `brew install ffmpeg` installs 8.x.
- **French entity-fishing KB.** The NER stage needs the French knowledge base (`db-fr`) loaded into the entity-fishing instance passed via `--ner-api-endpoint`. Without it, NER still runs and finds mentions but returns no `wikidataId`. spaCy needs `fr_core_news_md` (`python -m spacy download fr_core_news_md`).
- **No JavaScript needed.** Contrary to the research notes, the CRV page exposes the HLS master URL directly in server-rendered HTML — the Vodalys player is not in the loop. The `interventions-video` index ignores `limit > 12` (it collapses to a few highlighted cards) and interleaves commission réunions (`…IDC…`) between séance ones (`…IDS…`), so the resolver pages in 12s and only stops on a truly empty page.
- **Rate limiting.** `assemblee-nationale.fr` returns 429s on rapid access; [`scraper/common.py`](scraper/common.py) retries with exponential backoff and a polite `User-Agent`.

## Known limitations

- **Scope.** Legislature 17, Assemblée nationale, *séance publique* (plenary) only. The Sénat and commissions are out of scope (commission videos are skipped by the `IDS` filter in the media resolver).
- **`stime` lags video sync.** The AN adds `<texte stime>` only after it synchronises the séance video (typically the next day). For a just-held séance the compte rendu has no `stime`, so the merger falls back to the séance start for every speech's `dateStart` and `startOffset = 0` (no per-speech offsets); a later re-run picks up the offsets once the AN publishes them. `debug.stime` records the per-speech value (or `null`).
- **NEL gaps are non-deputies.** Député coverage is high (~91% of speeches on a typical séance); the misses are government ministers who aren't sitting deputies, senators speaking as ministers, and auditioned guests (e.g. company executives) — none are in the AMO *deputies* roster. `entity_dump_url` is empty in [`manifest.yaml`](manifest.yaml); run `python -m optv.parliaments.FR.scraper.build_entity_dump <data_dir>` once (or when the roster changes) to (re)build `metadata/entities.json`. The `_download` hook does this automatically when the file is missing.
- **Media resolution is index-depth-bound.** Resolving a séance's video pages the `interventions-video` index newest-first until the réunion id is found. Fresh séances (the production cron case) sit on page 1; back-filling an old séance can take many pages (`INDEX_MAX_PAGES` caps the search). Resolved `RU → CRV` pairs are cached in `original/media/_crv_index.json`.
- **Final-speech duration is estimated.** The last speech of a séance has no following `stime`, so its `duration` is the median of the séance's other speeches (capped at 120 s).
