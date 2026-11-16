# Congreso de los Diputados (ES)

Parser/merger for the plenary of the Spanish lower house (XV Legislature). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, merged per session. **Media is the authoritative spine**: every output speech keeps its video, proceedings turns with no video (chair interjections) are dropped.

- **Media stream** ([`scraper/fetch_interventions.py`](scraper/fetch_interventions.py) → [`parsers/media2json.py`](parsers/media2json.py)): the open-data interventions feed (`IntervencionesCronologicamente__<ts>.json`). One record per speech with a direct MP4 (`ENLACEDESCARGADIRECTA`), speaker (`ORADOR` / `CARGOORADOR`), agenda (`OBJETOINICIATIVA`) and HH:MM timing. The feed holds only the legislature in progress; the raw files of past legislatures are kept and never re-fetched.
- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): the per-session HTML Diario de Sesiones ("texto íntegro"). The feed only links text per session (~40 speeches per document), so one HTML is fetched per session and segmented by speaker markers (`El señor/La señora <ROLE/SURNAME> (Surname):`), tracking page anchors.

## Session numbering

The Diario series (`DSCD-<leg>-PL-<n>`) covers the Pleno and the Diputación Permanente and numbers their sittings in one sequence, so the Diario number is not the plenary session number (Diario 184 holds plenary session 178). The scraper groups the feed records by Diario and reads the plenary session number from the MP4 path (`{leg}_000400_{session}_…`, organ `400` = Pleno); session keys (`15178`) and `session.number` use it. The Diario id stays the text document id: it is in the raw media `meta.docId`, the proceedings `originTextID` and the PDF `sourceURI`. Diarios of the Diputación Permanente (organ `500`, own sitting numbers) and plenary Diarios without any per-speech video are skipped. The proceedings parser warns when the Diario header (`Sesión plenaria núm. N`) disagrees.

## Media links

- `media.videoFileURI` is the feed's MP4 path on `static.congreso.es`, the host the audiovisual archive links; the feed's `www.congreso.es` URLs refuse non-browser clients (alignment downloads).
- `media.sourcePage` is `https://app.congreso.es/v1/{leg}{intervention id}I`, built from the intervention id in the MP4 file name; it opens the sitting's video page at the intervention. The feed's own `ENLACEDIFERIDO` drops the first digit of the id.
- An intervention recorded in two clips (e.g. a minister's answer and rejoinder in question time) has both MP4 URLs, newline-separated, in one record. Each clip becomes its own speech (`debug.clipIndex` / `clipCount`); the second is placed after the clip with the next-lower intervention id, i.e. after the turns recorded in between. Records without video carry a placeholder URL (`…/leg/400//cortes/.mp4` or `…/leg__/400/__10000000/cortes/__10000000.mp4`) and are not media items.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) does global alignment on speaker **surname in sequence**. After the Needleman-Wunsch backtrack the matcher walks bidirectionally outward, absorbing same-surname proceeding turns and stepping past chair-mode interjections (`_is_chair_role`), so a speaker's body + chair-aside + closing reunite under one media clip.

## Running

```bash
./optv/parliaments/ES/workflow.py --period=15 <data_dir> \
    --download-original --merge-speeches --align-sentences --extract-entities \
    --limit-session '15001'
```

`./update <data_dir>` bakes in `--period=15` and the full stage set. `make download && make` mirrors the mtime-driven flow.

## Access notes

www.congreso.es (open data, Diario HTML, the feed's MP4 links) sits behind Akamai and answers non-browser `User-Agent`s with 403 "Access Denied". [`scraper/common.py`](scraper/common.py) carries a browser `User-Agent` plus an in-memory cookie jar (the HTML "texto íntegro" view round-trips a cookie). No challenge solving is required. `app.congreso.es` (archive sitting pages) and `static.congreso.es` (MP4s) serve any client.

## Known limitations

- **Role-only chair turns.** When the presiding officer speaks logged only as `La señora PRESIDENTA:` (no surname), the turn cannot be surname-matched to its media clip, so that clip publishes with video but no text (`debug.confidence = 0.5`, `debug.confidenceReason = no-matched-text`). Substantive speeches (which always carry a surname) match at ~95%. A future improvement could resolve role-only president turns to the current presiding officer.
- **Coarse timing.** `INICIO/FININTERVENCION` are HH:MM only; sentence-level timing comes from the shared aeneas `align` stage run on each speech's MP4.
- **NEL.** `nel` is in `manifest.supported_stages` and works as soon as an `entities.json` is present under `<data_dir>/metadata/`. The 2026-05-23 first dump was built directly from Wikidata SPARQL (`P39 = Q18171345` Spanish Congress MPs ever, 3 830 persons + 11 hand-mapped XV parliamentary groups; person-mention coverage on audited sessions ~55 %, gap is Wikidata's incomplete XV-term tagging, not the pipeline). `entity_dump_url` is empty until a hosted dump exists; with it set, `workflow.py --update-nel-entities` pulls a fresh copy in place.
- **Scope.** Pleno (plenary) only; the Diputación Permanente, committees, joint sessions and the Senado are out of scope. The audiovisual archive has per-speech video from the X legislature on, but the adapter reads the open data feed, which holds only the legislature in progress.
- **Joint debates repeat speeches.** The feed lists an intervention in a joint debate once per initiative (`NUMEXPEDIENTE`), with the same MP4, speaker and times, so such speeches appear more than once (`semantic.media.sourcePage.duplicate` warnings).
- **Two audio tracks.** Each MP4 carries the floor audio (`qaa`, first track) and the Spanish interpretation (`spa`). `align` uses the first track, so the Spanish version of a Catalan / Galician / Basque speech is aligned against the original-language audio.
- **Dual-language proceedings.** Speakers using a co-official language (Catalan / Galician / Basque) appear in the Diario with their text twice: the original co-language version followed by the official Spanish translation. This doubles `chars_p` for ~20 % of speeches in debates featuring those groups. The published text is complete and correct; a future feature could split language passes into separate `textBody` elements (`language: ca/gl/eu` vs `es`).
- **Proceedings fetch ignores `--limit-session`.** `download_proceedings_period` iterates every session found in `raw-*-media.json`. One-time per legislature (~3 minutes wall for a full legislature). Pre-filter the raw media dir if narrower scope is needed.
