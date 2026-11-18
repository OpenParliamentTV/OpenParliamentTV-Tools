# Sächsischer Landtag (DE-SN)

Pipeline for the Saxony state parliament. Built on a per-speech video spine
(speaker/faction/agenda metadata); the Plenarprotokoll PDF is now joined onto the
spine via an **experimental** PDF→TEI text path (see Known limitations). See
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Single media spine, no proceedings stream.

- **Media**: [scraper/fetch_media.py](scraper/fetch_media.py) →
  [parsers/media2json.py](parsers/media2json.py) → `original/media/`.

The Mediathek plenary-video archive (`www.landtag.sachsen.de/de/.../plenarvideos/index.cshtml?electoral_term_id={wp}&start=N`) is a single paginated list (20 items/page), sorted by sitting number (highest first) and by time within a sitting, where **each list item is fully self-contained**: one `<article class="xm_teaser …">` per speech carrying the speaker (natural `Firstname Lastname` order), the faction badge, the speech-time category, the TOP number + the agenda item's title (`thema`), the Sitzungsnummer, the date + wall-clock time, the daily HLS `<source>` URL (`stream-o01.envia-tel.net/vod/smil:{YYYYMMDD}.smil/playlist.m3u8`) and per-speech `startPosition`/`endPosition` offsets (seconds into that daily stream). So one pagination pass yields every field — there is **no per-speech GET**.

The list's Wahlperiode filter files WP 8's constituent sitting (2024-10-01) under WP 7, at the end of the WP 7 list after WP 7's own first sitting. The scraper derives each item's WP from its date (`WP_START_DATES` in [scraper/common.py](scraper/common.py)), so a WP 8 run picks that sitting up from the last pages of the WP 7 list (`08001`) and a WP 7 run leaves it out.

DE-SN is the SE/DE-SH/DE-BW per-speech-offset model (one daily recording, per-speech windows via `#t=start,end`): both start and end offsets are present in the source (end offsets are clamped to the next speech's start in `media2json` so the windows are disjoint), and the item's wall-clock time gives a **real** `dateStart`/`dateEnd` (German local time with its Europe/Berlin UTC offset; `debug.timesAreVideoRelative = false`, the DE-HH/DE-NI/DE-NW sub-class). The Plenarprotokoll PDF is parsed via `optv.shared.pdf2tei` and joined onto the spine in the merger, so matched speeches carry verbatim `textContents` and `supported_stages` includes `align`/`ner` (see Known limitations).

## Merge strategy

Single-source translation — [merger/merge_session.py](merger/merge_session.py). The list items already carry per-speech segmentation, so there is no cross-source matching for the spine itself (no Needleman-Wunsch): each speech becomes one Stage 2 record, then the PDF→TEI text is joined on (`join_text_to_spine`). The list has no chair turns (the session page lists them, but without a name), so the presiding officer comes from the protocol: each matched speech lists the chair(s) named in its text in `people[]` (context `president` / `vice-president`), and the chair's lines in `textBody` carry the chair's name. Sessions are keyed `{wp:02d}{sitzung:03d}` (e.g. `08025`); `electoralPeriod.number = 8`, `session.number = Sitzung`. Pure two-level hierarchy (Wahlperiode > Sitzung) — no dropped intra-term level.

## Running

```sh
# Routine run (newest period): download, merge, link entities
./workflow.py /path/to/OpenParliamentTV-Data-DE-SN \
    --download-original --merge-speeches --link-entities

# One test session
./workflow.py --period=8 /path/to/OpenParliamentTV-Data-DE-SN \
    --download-original --merge-speeches --link-entities --limit-session '08025'

# build the local NEL entity dump (until the hosted URL exists)
python -m optv.shared.entity_dump_bootstrap DE-SN /path/to/OpenParliamentTV-Data-DE-SN
```

`--limit-session` is an exact id / regex prefix (e.g. `08025`); the scraper pages the WP archive from the highest sitting down and stops once it has paged past the requested Sitzung. `--max-pages` caps the walk.

## Known limitations

- **Experimental, unvalidated text path.** The Plenarprotokoll PDF is parsed via
  `optv.shared.pdf2tei` and joined onto the spine, and `align`/`ner` run on the
  result. None of this has been validated — there is no Whisper-QC/text-fidelity
  audit yet, and the PDF→TEI extraction and the text↔spine join still need
  refinement. **Not ready for platform integration.**
- **WP 8 only.** The archive also covers WP 7; `periods: [8]` for now.
- **Aktuelle Stunde is one agenda item.** The list item's `thema` is the agenda item's full official title with its Drucksache numbers (identical to the session page). An Aktuelle Stunde holds several debates under one TOP, so their speeches share one `agendaItem`; the per-debate titles are only in the REDAS debate records, which carry no TOP number.
- **NEL is Wikidata-only / name-based** (`P39 wd:Q17334379` + WP-8 parties). Names match the Wikidata labels and German aliases, so a member whose name changed needs the current name as an alias on Wikidata. Coverage misses current members lacking the `P39` statement and government ministers (`Staatsregierung`, no faction) — benign `faction.missing` warnings.
- **Multi-day Sitzungen.** A Plenarsitzung can span two calendar days (separate
  daily HLS streams); per-speech `date` handles this, but the test session
  `08025` is single-day.
