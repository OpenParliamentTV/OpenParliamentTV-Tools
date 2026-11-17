# Assembleia da República (PT)

Pipeline for the Portuguese Assembly of the Republic (plenary, *reuniões plenárias*). This directory implements the parliament-specific Stage 1 (scrape + parse + merge); the shared stages (NEL → align → NER → publish) come from `optv.shared.workflow`. See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide context. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Three input streams, joined per speech:

- **Media spine — av.parlamento.pt JSON API.** `scraper/fetch_media.py` → `original/media/{session}-av.json`; `parsers/media2json.py` → `original/media/{session}-media.json`. The per-meeting endpoint `/api/v1/videos/Plenary/{leg}/{sl}/{meeting}` lists one *intervention* per speech with speaker, role, party, `interventionType`, and `startTime`/`endTime` offsets into the session recording. **This is the authoritative spine** (it defines which speeches exist). Two sidecars per meeting:
  - `{session}-av-metadata.json` (`…/metadata/json`): a one-line summary per intervention, used for the agenda items.
  - `{session}-av-sources.json`: the recording URLs from the meeting page's `meetingVideoSources`.

  The per-speech video is a server-side-clipped HLS stream of that recording (`…/{file}.mp4/ClipFrom/{startMs}/ClipTo/{endMs}/index.m3u8`); the un-clipped session HLS is the audio source for alignment. The av `eventDate` is Lisbon wall-clock time, so `dateStart` carries the Europe/Lisbon offset.
- **Text — debates.parlamento.pt.** `scraper/fetch_proceedings.py` → `original/proceedings/{session}-dar.html`; `parsers/proceedings2json.py` → `original/proceedings/{session}-proceedings.json`. Appending `?sft=true` ("Texto Completo") to the DAR catalog URL returns the verbatim text as HTML: one `<p>` per PDF line inside one `waypointMarker` div per Diário page, with speaker markers `O/A Sr.(ª) Name (Party): —`; no PDF parsing needed. Each parsed turn records its pages.
- **Open data Intervenções.** `scraper/fetch_opendata.py` downloads the legislatura's Intervenções file (link found in the plain HTML of the parlamento.pt folder pages; raw copy in `cache/opendata/`, refreshed at most every 20 h) and writes `original/opendata/{session}-od.json`: per av intervention its Diário page range, the deputy's `idCadastro` and group, and the debate. Only reuniões the file covers get one.
- **NEL — Wikidata.** `scraper/build_entity_dump.py` → `metadata/entities.json` (members of the Assembly via `P39 wd:Q19953703`, one entry per Wikidata item; parties via `P102`, current ones pinned). Each member carries the Assembly deputy ID (`P6199`, the open data `idCadastro`) as `additionalInformation.originPersonID`, so NEL links a speaker the open data identifies by ID; otherwise by name, where a name several members share goes to the most recent member.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) joins the Diário text onto the av spine in three steps.

1. **Speech units.** The Diário records interjections from the floor (*apartes*: "Muito bem!") as speaker turns of their own, and a speech interrupted by them continues under a repeated marker of the same speaker (often opening with `…`). A unit is one speaker's run of turns plus the apartes inside it; a chair turn ends a unit.
2. **Join.** An equal-key Needleman-Wunsch (`optv.shared.sequence_align`) matches av interventions to units by the speaker's surname (office holders such as the Primeiro-Ministro resolved to their name, which the Diário gives only at the office's first marker), or by the chair key for the presiding officer. Matching first names are required where both sides give one, so namesakes stay apart. A pair must also be plausible: an intervention longer than 20 s needs at least 3 characters of text per second, which keeps an aparte off a speech. Where the open data record gives the intervention's Diário pages, the unit must lie on them.
3. **Absorption.** Each matched intervention takes the speaker's unmatched units next to it (across unmatched chair remarks and apartes), e.g. a speech the chair interrupted.

The text body lists the speaker's turns as `speech` (`main-speaker`), chair remarks inside the clip as `speech` (`president`) and apartes as `comment` blocks (`speaker`, text `(Name [Party]: …)`), which the aligner skips. `people[].originPersonID` is the open data `idCadastro`. A speech whose av intervention finds no text publishes with `textContents: []` (align/NER skip it), so the media half always ships.

**Agenda items.** The sitting description lists the items joined by ` / `. Each intervention gets the item whose words its summary shares, along a path that only moves forward through the items; `Votações` interventions go to the votes item, and interventions without a matching word stay with the previous item. Without any usable summary, every speech gets the whole description.

## Running

```
# Whole legislatura (enumerates reuniões from av.parlamento.pt):
./optv/parliaments/PT/update ../../../../OpenParliamentTV-Data-PT

# Or explicitly, one or more reuniões, all stages:
./optv/parliaments/PT/workflow.py --period=17 <data_dir> \
    --session 17-1-059 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities \
    --ner-api-endpoint http://localhost:8090/service
```

`--session {leg}-{sl}-{meeting}` (repeatable) scopes to specific reuniões; `--limit-session <regex>` filters; `--period` is the legislatura.

## Access notes

- av.parlamento.pt (Metatheke) exposes an **undocumented** REST API; the URL patterns here were reverse-engineered. No auth, occasional 5xx (retried).
- The DAR text lives behind `debates.parlamento.pt/...?sft=true`; the bare catalog page is a pdf.js viewer and is **not** the text source. The text is extracted automatically from the PDF, so the parser accepts marker variants (`Sr.a` for `Sr.ª`, a missing colon or parenthesis, `⎯` for the dash, names without the space).
- The open data file links carry encrypted path parameters; the scraper finds them on the folder pages instead of storing them.
- NER needs the Portuguese KB (`db-pt`, `sciencialab/entity-fishing-db-pt` on Hugging Face) loaded into the entity-fishing endpoint (`--ner-api-endpoint`); the Conductor's `docker-compose.yml` mounts it. The manifest turns on the NER corrections (`ner.corrections`, [optv/shared/ner_corrections.py](../../shared/ner_corrections.py)): forms of address ("Srs.", "Deputados") and links to name or letter pages are dropped, and party abbreviations and the sitting's speakers take their ID from the entity dump ("CH" → Chega).

## Known limitations

- **Legislatura 17 only** (`periods: [17]`); earlier legislaturas (per-speech video exists since ~2005) are not wired.
- **Freshness.** The av archive adds a sitting about five weeks after it, the open data records arrive about six weeks after it, and the Diário text a few weeks after it. A sitting enters the pipeline only once av lists it.
- **Open data coverage.** The XVII Intervenções file starts with the second sessão legislativa (2026-09-16), so earlier reuniões are joined without page anchors; the chair's procedural interventions have no record.
- **Text–video join is a sequence alignment, not a hard key.** A clip with long interruptions can carry little text for its length; a turn whose speaker marker the automatic text mangles beyond the accepted variants stays inside the previous turn.
- **Agenda items** follow the sitting description: a debate the description does not list inherits the previous item's title, and items debated jointly share the first item's title.
- **NEL is ID-based only where the open data identifies the speaker**; elsewhere it matches names, so hyphenated source names and people who are not members (most government members) can miss; a miss is a warning, not an error.
