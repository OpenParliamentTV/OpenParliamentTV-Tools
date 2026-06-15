# Congreso de los Diputados (ES)

Parser/merger for the Spanish lower house (XV Legislature, the only term with per-speech video). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, merged per session. **Media is the authoritative spine**: every output speech keeps its video, proceedings turns with no video (chair interjections) are dropped.

- **Media stream** ([`scraper/fetch_interventions.py`](scraper/fetch_interventions.py) → [`parsers/media2json.py`](parsers/media2json.py)): the open-data interventions feed (`IntervencionesCronologicamente__<ts>.json`). Rich and clean — one record per speech with a direct MP4 (`ENLACEDESCARGADIRECTA`), speaker (`ORADOR` / `CARGOORADOR`), agenda (`OBJETOINICIATIVA`) and HH:MM timing.
- **Proceedings stream** ([`scraper/fetch_proceedings.py`](scraper/fetch_proceedings.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): the per-session HTML Diario de Sesiones ("texto íntegro"). The feed only links text per session (~40 speeches per document), so one HTML is fetched per session and segmented by speaker markers (`El señor/La señora <ROLE/SURNAME> (Surname):`), tracking page anchors.

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

congreso.es 403s bare requests. [`scraper/common.py`](scraper/common.py) carries a browser `User-Agent` (for the JSON/CDN) plus an in-memory cookie jar (the HTML "texto íntegro" view round-trips a cookie). No challenge solving is required.

## Known limitations

- **Role-only chair turns.** When the presiding officer speaks logged only as `La señora PRESIDENTA:` (no surname), the turn cannot be surname-matched to its media clip, so that clip publishes with video but no text (`debug.confidence = 0.5`, `debug.confidenceReason = no-matched-text`). Substantive speeches (which always carry a surname) match at ~95%. A future improvement could resolve role-only president turns to the current presiding officer.
- **Coarse timing.** `INICIO/FININTERVENCION` are HH:MM only; sentence-level timing comes from the shared aeneas `align` stage run on each speech's MP4.
- **NEL.** `nel` is in `manifest.supported_stages` and works as soon as an `entities.json` is present under `<data_dir>/metadata/`. The 2026-05-23 first dump was built directly from Wikidata SPARQL (`P39 = Q18171345` Spanish Congress MPs ever, 3 830 persons + 11 hand-mapped XV parliamentary groups; person-mention coverage on audited sessions ~55 %, gap is Wikidata's incomplete XV-term tagging, not the pipeline). `entity_dump_url` is empty until a hosted dump exists; with it set, `workflow.py --update-nel-entities` pulls a fresh copy in place.
- **Scope.** Pleno (plenary) only; committees, joint sessions and the Senado are out of scope.
- **Dual-language proceedings.** Speakers using a co-official language (Catalan / Galician / Basque) appear in the Diario with their text twice: the original co-language version followed by the official Spanish translation. This doubles `chars_p` for ~20 % of speeches in debates featuring those groups. The published text is complete and correct; a future feature could split language passes into separate `textBody` elements (`language: ca/gl/eu` vs `es`).
- **Proceedings fetch ignores `--limit-session`.** `download_proceedings_period` iterates every session found in `raw-*-media.json`. One-time per legislature (~3 minutes wall on 184 sessions). Pre-filter the raw media dir if narrower scope is needed.
