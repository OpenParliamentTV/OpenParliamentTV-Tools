# Assembleia da República (PT)

Pipeline for the Portuguese Assembly of the Republic (plenary, *reuniões
plenárias*). This directory implements the parliament-specific Stage 1 (scrape +
parse + merge); the shared stages (NEL → align → NER → publish) come from
`optv.shared.workflow`. See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md)
for repo-wide context. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two input streams, joined per speech:

- **Media spine — av.parlamento.pt JSON API.** `scraper/fetch_media.py` →
  `original/media/{session}-av.json`; `parsers/media2json.py` →
  `original/media/{session}-media.json`. The per-meeting endpoint
  `/api/v1/videos/Plenary/{leg}/{sl}/{meeting}` lists one *intervention* per
  speech with speaker, party, `interventionType`, and `startTime`/`endTime`
  offsets into the session recording. **This is the authoritative spine** (it
  defines which speeches exist). The per-speech video is a server-side-clipped
  HLS stream built from the offsets
  (`…/{session}.mp4/ClipFrom/{startMs}/ClipTo/{endMs}/index.m3u8`); the
  un-clipped session HLS is the audio source for alignment.
- **Text — debates.parlamento.pt.** `scraper/fetch_proceedings.py` →
  `original/proceedings/{session}-dar.html`; `parsers/proceedings2json.py` →
  `original/proceedings/{session}-proceedings.json`. Appending `?sft=true`
  ("Texto Completo") to the DAR catalog URL returns the verbatim text as HTML
  (`<p>` paragraphs with inline `O/A Sr.(ª) Name (Party):` speaker markers); no
  PDF parsing needed.
- **NEL — Wikidata.** `scraper/build_entity_dump.py` → `metadata/entities.json`
  (members of the Assembly via `P39 wd:Q19953703`; parties via `P102`). The av
  JSON carries no Wikidata/BID, so the dump is Wikidata-only.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) walks the av intervention
spine and grafts the matching DAR text via a **Needleman-Wunsch alignment** of
per-turn match keys — the speaker's surname for deputies, or a canonical role
(`presidente` / `secretario` / `ministro`) for the chair/officers — because the
text is finer-grained than the av list (it interleaves chair interjections the
av list does not enumerate). A speech whose av intervention finds no text match
publishes with `textContents: []` (align/NER skip it), so the media half always
ships. This is the DE two-source pattern with a PT-specific join key.

## Running

```
# Whole legislatura (enumerates reuniões from av.parlamento.pt):
./optv/parliaments/PT/update ../../../../OpenParliamentTV-Data-PT

# Or explicitly, one or more reuniões, all stages:
./optv/parliaments/PT/workflow.py --period=17 <data_dir> \
    --session 17-1-059 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

`--session {leg}-{sl}-{meeting}` (repeatable) scopes to specific reuniões;
`--limit-session <regex>` filters; `--period` is the legislatura.

## Access notes

- av.parlamento.pt (Metatheke) exposes an **undocumented** REST API; the URL
  patterns here were reverse-engineered. No auth, occasional 5xx (retried).
- The DAR text lives behind `debates.parlamento.pt/...?sft=true`; the bare
  catalog page is a pdf.js viewer and is **not** the text source.
- NER needs the Portuguese KB (`db-pt`) loaded into the entity-fishing endpoint
  (`--ner-api-endpoint`).

## Known limitations

- **Legislatura 17 only** (`periods: [17]`); earlier legislaturas (per-speech
  video exists since ~2005) are not yet wired.
- **Text–video join is a sequence alignment, not a hard key.** The av
  "intervention number" and the DAR text are linked only as an index cross-ref;
  brief procedural turns (Protesto, chair interjections) may not match and ship
  text-less (`debug.textTurnIndex` absent). Substantive deputy speeches match
  reliably by surname.
- **NEL coverage is name-based** (no source-side person ID), so hyphenated or
  all-caps source names can miss; a miss is a warning, not an error.
- **Bilingual edge cases**: speeches are Portuguese; the pipeline runs uniformly
  in `pt`.
