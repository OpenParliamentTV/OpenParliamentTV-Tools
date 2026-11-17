# Assemblée nationale (FR)

Parser/merger for the French National Assembly plenary (legislature 17, 2024–present). Built around the official Assemblée nationale open data (Syceron comptes rendus + AMO acteurs/organes) and the `videos-diffusion.assemblee-nationale.fr` HLS stream. The compte rendu carries the verbatim text **and** the per-speech video offsets, so the merge is a single-source spine; only the séance video URL and the recording's start time are resolved externally. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, but the proceedings stream is the **authoritative spine** — it supplies both text and timing. Session key is the Syceron compte-rendu uid suffix (e.g. `2026O1N254`); see [`common.py`](common.py) for the uid ↔ `session.number` encoding (the integer packs year + session-type + séance number so it stays unique across a legislature).

- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): per-séance Syceron compte rendu (`CRSANR5L17S{year}{O|E}{ordre}N{num}.xml`). Targeted fetches use the per-document endpoint `/dyn/opendata/{uid}.xml`; the default bulk mode pulls the legislature's `…/vp/syceronbrut/syseron.xml.zip` (rebuilt daily) and extracts every `CRS*.xml`. Both modes rewrite a local file only when the published bytes differ, so republished comptes rendus are picked up and unchanged ones keep their mtime. The namespaced XML gives `<paragraphe id_acteur=…>` speeches, `<orateur>` speaker labels, the `<point nivpoint=…>` agenda tree, `<etat>` (`provisoire` / `complet`) and, in the complete version, the per-speech `<texte stime="26.68">` offset (seconds from the start of the séance recording). Consecutive paragraphes by the same `id_acteur` within one agenda point are folded into a single speech; `code_style ≠ NORMAL` paragraphes (vote tallies, "(Applaudissements…)") become `comment` text bodies.
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): one HLS video per séance, from the HTML compte rendu `/dyn/17/comptes-rendus/seance/{uid}` (one request per séance). Its séance button carries the HLS master playlist URL, whose file name `hemi_YYYYMMDDHHMMSS` is the recording's start in Paris local time; every intervention block (`<div id="{id_syceron}">`) carries the same URL and its offset (`data-contentParameters='{"currentTime": …}'`), which the resolver compares with the local XML `stime` as a staleness check. Before storing the descriptor it checks that the recording is still online. [`align_prep.py`](align_prep.py) downloads that séance track once and slices per-speech MP3s with ffmpeg for aeneas.
- **Entities** ([`scraper/build_entity_dump.py`](scraper/build_entity_dump.py)): AMO10 (`deputes_actifs_mandats_actifs_organes`) ⋈ Wikidata SPARQL (P4123 = AN id) → `metadata/entities.json` (NEL) + `metadata/acteurs.json` (the `id_acteur → groupe` map the proceedings parser needs, since the compte rendu names the speaker but not their group).

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) does a **single-source spine walk** — no cross-source matching is needed because text and timing come from the same document. Speeches are emitted in document order; each speech's `stime` becomes `media.additionalInformation.startOffset`, and `duration` runs to the next speech's offset (median-based fallback for the last speech of the séance). The séance's one HLS URL is attached to every speech as a `#t=start,end` fragment, and `originMediaID` is the recording id (`hemi_…`). A speech's `dateStart` is the recording start plus its offset, and `dateEnd` adds the duration (Europe/Paris, DST-aware); the recording starts 10 to 15 minutes before the sitting opens, so the scheduled `<dateSeance>` is only the fallback anchor when no recording start is known. `sourcePage` is the compte rendu page anchored at the speech's first intervention (`…/seance/{uid}#{id_syceron}`), which offers playback from that point and is unique per speech.

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
- **No JavaScript needed.** The compte rendu page is server-rendered and exposes the HLS master URL and the per-intervention offsets directly. `videos-diffusion.assemblee-nationale.fr` redirects to the vendor's CDN host `videos-an.vodalys.com`, where the recordings are stored.
- **Rate limiting.** `assemblee-nationale.fr` returns 429s on rapid access; [`scraper/common.py`](scraper/common.py) retries with exponential backoff and a polite `User-Agent`.

## Known limitations

- **Scope.** Legislature 17, Assemblée nationale, *séance publique* (plenary) only. The Sénat and commissions are out of scope (commission videos are skipped by the `IDS` filter in the media resolver).
- **Provisional comptes rendus have no offsets.** The first version of a compte rendu, published on the evening of the sitting, is `<etat>provisoire</etat>` and has no `stime`; the complete version adds them, and later republications can move offsets by several seconds and correct text. The media resolver writes no descriptor for a compte rendu without offsets, so such a sitting is not merged until a later run fetches the complete version (`debug.stime` records the per-speech value). A few complete comptes rendus have no offsets at all; their sittings are never merged. The open-data daily publication list (`/dyn/opendata/list-publication/publication_{YYYY-MM-DD}.csv`) names every (re)published document with its time.
- **Video retention is about 10–11 months.** Older recordings are no longer streamable (HTTP 404); the portal releases them only on request by e-mail, for three days. Sittings must therefore be imported within that window, the bulk media resolver requests nothing for sittings older than a year (`VIDEO_MAX_AGE_DAYS`), and a recording found offline is listed in `original/media/_unavailable.json` and not requested again. Published speeches lose playable video once their recording goes offline, because `videoFileURI` points at the AN's stream; the pipeline does not mirror it. Only sittings with a resolved video descriptor are in scope for merging.
- **NEL gaps are non-deputies.** Député coverage is high (~91% of speeches on a typical séance); the misses are government ministers who aren't sitting deputies, senators speaking as ministers, and auditioned guests (e.g. company executives) — none are in the AMO *deputies* roster. `entity_dump_url` is empty in [`manifest.yaml`](manifest.yaml); run `python -m optv.parliaments.FR.scraper.build_entity_dump <data_dir>` once (or when the roster changes) to (re)build `metadata/entities.json`. The `_download` hook does this automatically when the file is missing.
- **Final-speech duration is estimated.** The last speech of a séance has no following `stime`, so its `duration` is the median of the séance's other speeches (capped at 120 s).
