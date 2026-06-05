# Sächsischer Landtag (DE-SN)

Pipeline for the Saxony state parliament. Video-only v1: per-speech video +
speaker/faction/agenda metadata, no transcript text yet. See
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Single media spine, no proceedings stream.

- **Media**: [scraper/fetch_media.py](scraper/fetch_media.py) →
  [parsers/media2json.py](parsers/media2json.py) → `original/media/`.

The Mediathek plenary-video archive
(`www.landtag.sachsen.de/de/.../plenarvideos/index.cshtml?electoral_term_id={wp}&start=N`)
is a single paginated list (20 items/page) where **each list item is fully
self-contained**: one `<article class="xm_teaser …">` per speech carrying the
speaker (natural `Firstname Lastname` order), the faction badge, the speech-time
category, the TOP number + a short `thema` text, the Sitzungsnummer, the date +
wall-clock time, the daily HLS `<source>` URL
(`stream-o01.envia-tel.net/vod/smil:{YYYYMMDD}.smil/playlist.m3u8`) and per-speech
`startPosition`/`endPosition` offsets (seconds into that daily stream). So one
pagination pass yields every field — there is **no per-speech GET**.

DE-SN is the SE/DE-SH/DE-BW per-speech-offset model (one daily recording,
per-speech windows via `#t=start,end`), but cleaner: both start and end offsets
are present (no end-synthesis), and the item's wall-clock time gives a **real**
`dateStart`/`dateEnd` (`debug.timesAreVideoRelative = false`, the DE-HH/DE-NI/DE-NW
sub-class). Verbatim text lives only in PDF Plenarprotokolle (§5 Abs. 2 UrhG, free
to reuse) — blocked by tooling, not licensing — so `textContents` is empty until a
PDF parser lands and `align`/`ner` are omitted from `supported_stages`.

## Merge strategy

Single-source translation — [merger/merge_session.py](merger/merge_session.py).
The list items already carry per-speech segmentation, so there is no
cross-source matching (no Needleman-Wunsch): each speech becomes one Stage 2
record with `textContents: []`. Sessions are keyed `{wp:02d}{sitzung:03d}`
(e.g. `08025`); `electoralPeriod.number = 8`, `session.number = Sitzung`. Pure
two-level hierarchy (Wahlperiode > Sitzung) — no dropped intra-term level.

## Running

```sh
# one-shot wrapper (period 8 baked in)
./update /path/to/OpenParliamentTV-Data-DE-SN

# or the explicit workflow (one test session)
./workflow.py --period=8 /path/to/OpenParliamentTV-Data-DE-SN \
    --download-original --merge-speeches --link-entities --limit-session '08025'

# build the local NEL entity dump (until the hosted URL exists)
python -m optv.parliaments.DE-SN.scraper.build_entity_dump /path/to/OpenParliamentTV-Data-DE-SN
```

`--limit-session` is an exact id / regex prefix (e.g. `08025`); the scraper pages
the WP archive newest-first and stops once it has paged past the requested
Sitzung. `--max-pages` caps the walk.

## Known limitations

- **Video-only.** `textContents: []`; `align`/`ner` omitted (no PDF parser). The
  platform shows clips with speaker / faction / agenda metadata, no transcript.
- **WP 8 only.** The archive also covers WP 7; `periods: [8]` for now.
- **Thin agenda titles.** The list item carries a TOP number + a short `thema`
  text, not the full Plenarprotokoll TOP title; `agendaItem.title` is that short
  text (or `TOP {n}`). A future PDF/Tagesordnung pass could enrich it.
- **NEL is Wikidata-only / name-based** (`P39 wd:Q17334379` + WP-8 parties).
  Coverage misses current members lacking the `P39` statement and government
  ministers (`Staatsregierung`, no faction) — benign `faction.missing` warnings.
- **Multi-day Sitzungen.** A Plenarsitzung can span two calendar days (separate
  daily HLS streams); per-speech `date` handles this, but the test session
  `08025` is single-day.
