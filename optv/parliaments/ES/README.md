# Congreso de los Diputados (ES)

Parser/merger for the plenary of the Spanish lower house (XV Legislature). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, merged per session. **Media is the authoritative spine**: every output speech keeps its video, proceedings turns with no video (chair interjections) are dropped.

- **Media stream** ([`scraper/fetch_interventions.py`](scraper/fetch_interventions.py) → [`parsers/media2json.py`](parsers/media2json.py)): the open-data interventions feed (`IntervencionesCronologicamente__<ts>.json`). One record per speech with a direct MP4 (`ENLACEDESCARGADIRECTA`), speaker (`ORADOR` / `CARGOORADOR`), agenda (`OBJETOINICIATIVA`) and HH:MM timing. The feed holds only the legislature in progress; the raw files of past legislatures are kept and never re-fetched.
- **Media fallback** ([`scraper/fetch_search.py`](scraper/fetch_search.py)): when the feed cannot be downloaded, the same records come from the intervention search (Búsqueda de intervenciones). Its result list is a JSON resource (POST `filtrarListado`, 25 interventions per page) backed by the same database, filtered by legislature, date range and organ `Pleno`, so a sitting takes about five requests, paced two seconds apart. The entries are mapped onto the feed's fields (times keep their seconds, so they are not `coarseTiming`); the list starts at the newest sitting on file. `meta.source` in a raw file says where it came from (`opendata` / `search`): fallback records never replace a file from the feed, and the feed replaces fallback files once it is back. `fetch_interventions.py --source search [--since dd/mm/yyyy]` uses the search directly.
- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): the per-session HTML Diario de Sesiones ("texto íntegro"). The feed only links text per session (~40 speeches per document), so one HTML is fetched per session and segmented by speaker markers (`El señor/La señora <ROLE/SURNAME> (Surname):`), tracking page anchors.

## Session numbering

The Diario series (`DSCD-<leg>-PL-<n>`) covers the Pleno and the Diputación Permanente and numbers their sittings in one sequence, so the Diario number is not the plenary session number (Diario 184 holds plenary session 178). The scraper groups the feed records by Diario and reads the plenary session number from the MP4 path (`{leg}_000400_{session}_…`, organ `400` = Pleno); session keys (`15178`) and `session.number` use it. The Diario id stays the text document id: it is in the raw media `meta.docId`, the proceedings `originTextID` and the PDF `sourceURI`. Diarios of the Diputación Permanente (organ `500`, own sitting numbers) and plenary Diarios without any per-speech video are skipped. The proceedings parser warns when the Diario header (`Sesión plenaria núm. N`) disagrees.

## Media links

- `media.videoFileURI` is the feed's MP4 path on `static.congreso.es`, the host the audiovisual archive links; the feed's `www.congreso.es` URLs refuse non-browser clients (alignment downloads).
- `media.sourcePage` is `https://app.congreso.es/v1/{leg}{intervention id}I`, built from the intervention id in the MP4 file name; it opens the sitting's video page at the intervention. The feed's own `ENLACEDIFERIDO` drops the first digit of the id.
- An intervention recorded in two clips (e.g. a minister's answer and rejoinder in question time) has both MP4 URLs, newline-separated, in one record. Each clip becomes its own speech (`debug.clipIndex` / `clipCount`); the second is placed after the clip with the next-lower intervention id, i.e. after the turns recorded in between. Records without video carry a placeholder URL (`…/leg/400//cortes/.mp4` or `…/leg__/400/__10000000/cortes/__10000000.mp4`) and are not media items.
- **Joint debates.** The feed lists an intervention in a joint debate (debate conjunto) once per initiative: same MP4, speaker and times, different `OBJETOINICIATIVA` / `NUMEXPEDIENTE`. `media2json` keeps one speech per intervention, with the initiative of the lowest `NUMEXPEDIENTE` as `agendaItem`; the other initiatives are in `debug.jointInitiatives` (`numExpediente`, `title`).
- **Audio tracks.** Each MP4 carries the floor audio (`qaa`, stream 0) and the Spanish interpretation (`spa`, stream 1).

## Co-official languages

Speakers may use Catalan (incl. Valencian), Galician or Basque. The Diario prints each such passage as spoken, immediately followed by its official Spanish version, with no marker between the two; a speech may alternate, and passages spoken in Spanish are printed once. [`parsers/language.py`](parsers/language.py) labels each paragraph by function words that only one of the four languages uses, then pairs each co-official passage with the Spanish run after it of matching length (or, failing that, takes everything after the last co-official paragraph as the Spanish version of the whole turn). Such a turn gets two `textContents` entries, both covering the whole speech: the text as spoken (`language: ca` / `gl` / `eu`, Spanish passages included) and the Spanish version (`language: es`), with `originalLanguage` naming the first. A turn whose co-official text has no Spanish version in the Diario (the presiding officer's asides, for instance) keeps one `es` entry with the text as printed.

Alignment follows the renditions: the entry in `originalLanguage` is aligned against the floor audio, the Spanish version against the interpretation track (`audio_track` in [`workflow.py`](workflow.py), passed to the shared `align`).

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) does global alignment on speaker **surname in sequence** (accent-folded, hyphens read as spaces). After the Needleman-Wunsch backtrack the matcher walks bidirectionally outward, absorbing same-surname proceeding turns and stepping past chair-mode interjections (`_is_chair_role`), so a speaker's body + chair-aside + closing reunite under one media clip. The absorbed turns' text is joined into one entry per rendition (the platform stores one text per type and language).

A few clips per session have no matched text (role-only chair turns, oaths). `Config.TEXT_PROBE = "any"` keeps such sessions text-bearing for the align and NER stages, which skip the text-less speeches; a session is `no_text` only when no speech has text.

## Running

```bash
./optv/parliaments/ES/workflow.py --period=15 <data_dir> \
    --download-original --merge-speeches --align-sentences --extract-entities \
    --limit-session '15001'
```

`./update <data_dir>` bakes in `--period=15` and the full stage set. `make download && make` mirrors the mtime-driven flow.

## Access notes

www.congreso.es (open data, intervention search, Diario HTML, the feed's MP4 links) sits behind Akamai and answers non-browser `User-Agent`s with 403 "Access Denied". [`scraper/common.py`](scraper/common.py) carries a browser `User-Agent` plus an in-memory cookie jar (the HTML "texto íntegro" view round-trips a cookie). No challenge solving is required. `app.congreso.es` (archive sitting pages) and `static.congreso.es` (MP4s) serve any client.

## Known limitations

- **Role-only chair turns.** When the presiding officer speaks logged only as `La señora PRESIDENTA:` (no surname), the turn cannot be surname-matched to its media clip, so that clip publishes with video but no text (`debug.confidence = 0.5`, `debug.confidenceReason = no-matched-text`). Substantive speeches (which always carry a surname) match at ~95%. A future improvement could resolve role-only president turns to the current presiding officer.
- **Coarse timing.** The feed's `INICIO/FININTERVENCION` are HH:MM only (`debug.coarseTiming`), so `media.duration` from the feed is up to a minute off; the `align` stage replaces it with the clip's probed length. Sentence-level timing comes from the shared aeneas `align` stage run on each speech's MP4.
- **NEL.** `nel` is in `manifest.supported_stages` and works as soon as an `entities.json` is present under `<data_dir>/metadata/`. The 2026-05-23 first dump was built directly from Wikidata SPARQL (`P39 = Q18171345` Spanish Congress MPs ever, 3 830 persons + 11 hand-mapped XV parliamentary groups; person-mention coverage on audited sessions ~55 %, gap is Wikidata's incomplete XV-term tagging, not the pipeline). `entity_dump_url` is empty until a hosted dump exists; with it set, `workflow.py --update-nel-entities` pulls a fresh copy in place.
- **Scope.** Pleno (plenary) only; the Diputación Permanente, committees, joint sessions and the Senado are out of scope. The audiovisual archive has per-speech video from the X legislature on, but the adapter reads the open data feed, which holds only the legislature in progress.
- **Joint debates have one agenda item.** A speech in a joint debate carries the first initiative as `agendaItem` and the others only in `debug.jointInitiatives`, until the data model can hold several agenda items per speech.
- **Language detection is lexical.** Paragraph labels come from function-word counts; a co-official passage whose Spanish version differs much in length, or a Spanish quote inside a co-official paragraph, can leave a turn as one `es` entry with the text as printed. Aeneas reads the co-official text with the Spanish voice.
- **Proceedings fetch ignores `--limit-session`.** `download_proceedings_period` iterates every session found in `raw-*-media.json`. One-time per legislature (~3 minutes wall for a full legislature). Pre-filter the raw media dir if narrower scope is needed.
