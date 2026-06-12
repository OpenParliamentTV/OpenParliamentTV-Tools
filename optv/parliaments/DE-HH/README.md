# Hamburgische Bürgerschaft (DE-HH)

This directory implements the OpenParliamentTV pipeline for the Hamburgische
Bürgerschaft (WP 23). It is built on a per-speech video spine; the Plenarprotokoll
PDF is now joined onto that spine via an **experimental, unvalidated** PDF→TEI
text path (see below) — see
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context and the DE-SH / DE-BY / DE-BW READMEs for the same regime. Among these
German Landtage it is the richest media source: genuine per-TOP agenda,
**real per-speech wall-clock timestamps**, and source-native per-speech UUIDs. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

The media stream is the spine (the im-en.com mediathek session pages). The
Plenarprotokoll PDF (ParlDok) is parsed via `optv.shared.pdf2tei` and joined onto
that spine in the merger (`join_text_to_spine`), so matched speeches carry
verbatim `textContents` (unmatched keep `[]`) and `supported_stages` now includes
`align`/`ner` (see Known limitations).

- `scraper/fetch_archive.py` → the candidate session index
  (`metadata/archive-wp{N}.json`). Session URLs are fully predictable
  (`/sitzung/{WP}/{n}/`), so discovery is a `1..max` enumeration; the upper
  bound is read from the mediathek landing page (`--max-session` overrides it).
  Operator-supplied URLs (`--session` / `metadata/seed-urls.txt`) are merged
  in.
- `scraper/fetch_media.py` → fetches each session page and parses its static
  markup (`scraper/common.py:parse_session_page`) into
  `original/media/{session_id}-items.json` (agenda items + per-speech records).
  Candidate pages that 404 (gaps in the enumeration) are skipped.
- `parsers/media2json.py` → flattens to `original/media/{session_id}-media.json`
  (one record per speech: name split into chair-role prefix + `Firstname
  Lastname`, faction-vs-government-role from `data-speakerFunction`, clip-relative
  offsets, real wall-clock `start`/`end`).

The mediathek session page (`mediathek.buergerschaft-hh.de/sitzung/{WP}/{n}/`)
is **static HTML** carrying the full spine. Each Tagesordnungspunkt is a
`<video id="sessionitem-{UUID}">` whose `data-cleanStreamingSources` holds a
**server-side-clipped HLS master** (`/hls/clipFrom/{ms}/clipTo/{ms}/{date}/clean_{UUID}/…/master.m3u8`,
plus a sign-language `data-signStreamingSources` variant). Each speech is a
`<div class="speech">` with `data-speechPk` (UUID), `data-start`+`data-duration`
(seconds, **relative to that TOP's clip**), `data-sessionItemId`,
`data-speakerNameWithoutFunction`, `data-speakerFunction`, and a per-speech
`video-download/?start={unix}&stop={unix}` link whose timestamps give the real
wall-clock speech start/stop.

## Merge strategy

No cross-source matching: `merger/merge_session.py` is a single-source
translation pass from the per-speech intermediate records into Stage 2 (the
media stream is the authoritative spine). One `agendaItem` per TOP
(`id = "TOP-{n}"` when numbered, else a capped title slug, classified via
`optv.shared.agenda_types.classify_de_hh`), one `people[]` entry per speech, and
media as the TOP's HLS clip with an HTML5 media-fragment `#t=start,end` on
`videoFileURI` + `startOffset`/`endOffset` in `additionalInformation` (the
SE/DE-SH per-speech-offset model, per-TOP). `sourcePage` is made unique per
speech via the mediathek's own `#rede-{speechPk}` anchor (the platform keys
speech identity on it). `originID` is the source-native `speechPk` UUID. Speech
`dateStart`/`dateEnd` are **real wall-clock** (from the video-download
timestamps, UTC), so `debug.timesAreVideoRelative = false` — distinct from
DE-SH/DE-BY/DE-BW.

## Running

```sh
# Convenience wrapper (period 23 baked in)
./update /path/to/OpenParliamentTV-Data-DE-HH

# Pre-NEL, (re)generate the local entity dump (Wikidata P39 Q19360355 + WP-23 parties):
python optv/parliaments/DE-HH/scraper/build_entity_dump.py /path/to/OpenParliamentTV-Data-DE-HH

# Explicit, single test session:
./workflow.py --period=23 /path/to/OpenParliamentTV-Data-DE-HH \
    --download-original --merge-speeches --link-entities \
    --limit-session '23018' \
    --session 'https://mediathek.buergerschaft-hh.de/sitzung/23/18/'

# Validate the published output:
python -m optv.shared.validators.cli --dir /path/to/OpenParliamentTV-Data-DE-HH/processed --schema full
```

Session discovery enumerates `/sitzung/{period}/{n}/` for `n = 1..max`; the
result is cached in `archive-wp{N}.json`. The landing page only lists the
current term, so for an older term (WP 22) pass `--max-session N` or seed URLs.

## Access notes

The mediathek is plain public HTML (im-en.com, the same vendor as Niedersachsen
Plenar-TV) — no auth, no anti-bot. Everything the pipeline needs is in the
static page markup (no JSF ViewState like DE-BY, no result-cap AJAX like DE-SH).
ParlDok (the Parlamentsdatenbank, `buergerschaft-hh.de/parldok/`) is the unique
Hamburg system that cross-references proceedings and video, but it is a web
search interface serving PDF protocols only and is **not** used here.

## Known limitations

- **Experimental, unvalidated text path.** The Plenarprotokoll PDF (ParlDok) is
  parsed via `optv.shared.pdf2tei` and joined onto the spine, and `align`/`ner`
  run on the result. None of this has been validated — there is no
  Whisper-QC/text-fidelity audit yet, and the PDF→TEI extraction and the
  text↔spine join still need refinement. **Not ready for platform integration.**
- **NEL coverage caveat** (DE-SH/DE-BY/DE-BW class): the entity dump is built
  from Wikidata `P39 wd:Q19360355` ("member of the Hamburg Parliament"); it
  misses current WP-23 members lacking that statement and government members
  (`Senator(in)`, `Bürgermeister(in)`) who are not sitting MdHB. On the 23018
  smoke test person-NEL is 91/98 and faction-NEL 96/97. Fixable downstream by
  enriching the SPARQL or scraping the Bürgerschaft roster (abgeordnetenwatch
  parliament id 7, CC0).
- **`data-speakerFunction` is overloaded.** It is the faction for MPs (`SPD`,
  `GRÜNE`, `CDU`, `Die Linke`, `AfD`, `Fraktionslos`) but a government role for
  senators (`Senator`); the parser routes party values to `faction` and
  everything else to `role`. Chair roles (`Präsidentin` …) live in the name
  field and are split off there.
- **WP 23 only** (the current term). The mediathek covers WP 22 (2020–2025) and
  an archive back to 2017, but only WP 23 has been exercised.
