# Landtag Schleswig-Holstein (DE-SH)

Pipeline implementation for the Schleswig-Holstein Landtag mediathek
(`m7k.ltsh.de`). See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md)
for the cross-parliament onboarding flow. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two input streams *would* be the norm; for DE-SH only one is wired up:

- **Media stream** — `scraper/fetch_media.py` POSTs the m7k AJAX endpoint `result.php` once per Tagung (WP > Tagung > Sitzung is the source-side hierarchy) and saves the raw HTML response under `original/media/result/wp{N}/tagung-{NNN}.html`, plus one `iframe.php?b={id}` page per speech. `parsers/media2json.py` extracts one result block per speech into intermediate `20NNN-media.json`, carrying `b={id}`, speaker, faction (`Gruppe`), TOP, theme, start/end time-of-day, duration, plus the derived `videoFileURI` with the `#t=start,end` media-fragment URI. m7k marks a speech without a theme as `<div class="result crap">`; it is a speech like any other. Rows without a faction whose speaker is empty or the bare body "Präsidium" are chair time or breaks and are dropped.
- **Proceedings stream** — the Plenarprotokoll PDF is parsed via
  `optv.shared.pdf2tei` and joined onto the spine in the merger
  (`join_text_to_spine`); experimental — see Known limitations.

The **m7k AJAX feed is the spine** — it is already per-speech with
timestamps, IDs and speaker metadata, so no Needleman-Wunsch / fuzzy
alignment is needed. Sitting-day grouping happens in the merger.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) groups the
intermediate media records by `datum` (sitting day) and emits one Stage 2
record per speech; the PDF→TEI proceedings text is then joined onto that spine
(`join_text_to_spine`), so matched speeches carry verbatim `textContents` and
unmatched ones keep `[]`. The shared `classify_de_sh()` in
[`optv/shared/agenda_types.py`](../../shared/agenda_types.py) maps the
agenda title to a `(nativeType, type)` pair.

The agenda title is m7k's short `thema`. Some speeches have an empty `thema` or the `thema` "Präsidium" (then usually without a TOP number). Such a speech without a TOP takes the TOP of the speeches before and after it when both agree. Its title comes from another speech with the same TOP, else from the agenda title of the matched protocol text (the PDF bookmarks, see below), else `TOP n`. `debug.agendaSource` records which (`m7k`, `m7k-sibling`, `proceedings`, `top-number`).

`dateStart`/`dateEnd` are the list's clock times in Europe/Berlin with their UTC offset (`debug.timesAreVideoRelative = false`); `session.dateStart`/`dateEnd` span the whole sitting. The m7k media fragment runs from 3 s before to 3 s after these clock times.

The Plenarprotokoll's agenda titles come from its PDF bookmarks (`toc_layout="outline"` in [`parsers/pdf_config.py`](parsers/pdf_config.py)): level 2 holds the agenda items, level 3 every contribution, and items with no contribution between them are one joint debate titled by its first item. Protocols without bookmarks fall back to the printed table of contents.

The Tagung level is recorded in `meta.tagung` (matches DE-ST's
`meta.sitzungsperiode` pattern).

`session.number` is the Landtagssitzung number (1–119 in WP 20),
discovered from the Plenarprotokoll listing page
(`/infothek/wahl20/plenum/plenprot_seite/`) which uses the URL pattern
`20-{NNN}_{MM}-{YY}.pdf`. The mapping `date → Sitzung-number` is cached
to `metadata/sitzung_index.json` — no PDF download happens, just URL
discovery.

## Running

```sh
# Routine run (newest period): download, merge, link entities
./workflow.py /path/to/OpenParliamentTV-Data-DE-SH \
    --download-original --merge-speeches --link-entities
```

The routine run leaves out `--align-sentences` and `--extract-entities`; both stages are supported (see [manifest.yaml](manifest.yaml), `supported_stages`) but run on the experimental text path, so they are run explicitly. Pre-NEL the entity dump can be regenerated with:

```sh
python -m optv.shared.entity_dump_bootstrap DE-SH /path/to/OpenParliamentTV-Data-DE-SH
```

This writes `metadata/entities.json`; the NEL stage falls back to it
when the hosted dump at `de-sh.openparliament.tv` doesn't exist (it
currently doesn't).

## Access notes

`m7k.ltsh.de` is plain nginx with no anti-bot. The selectors and
`result.php` use jQuery `.load()` (POST, x-www-form-urlencoded). The
`wp` parameter takes **internal IDs** (4/5/6) for WP 18/19/20, not the
displayed Wahlperiode number; the `tg` parameter likewise takes a
3-digit internal ID. `result.php` is capped at **499 results per
query**, so the scraper iterates by Tagung (~10–40 speeches each, well
under the cap).

## Known limitations

- **Experimental, unvalidated text path.** The Plenarprotokoll PDF is parsed via
  `optv.shared.pdf2tei` and joined onto the spine, and `align`/`ner` run on the
  result. None of this has been validated — there is no Whisper-QC/text-fidelity
  audit yet, and the PDF→TEI extraction and the text↔spine join still need
  refinement. **Not ready for platform integration.**
- **Scope: WP 20 only.** WP 18 (from Jan 2014) and WP 19 are available on m7k but not onboarded. WP 19 needs only the manifest widened. WP 18 has no Plenarprotokoll listing page (`/infothek/wahl18/plenum/plenprot_seite/` is 404), so `fetch_archive` finds no date → Sitzung map and every WP 18 day would be skipped; it needs another source for the Sitzung numbers.
- **Non-German passages are tagged `de`.** Some speeches are in Low German (e.g. the Plattdüütsch debate in 20-123), and SSW members use Danish passages, some members North Frisian. The protocol prints them in the original language without a marker, so `originalLanguage` and `textContents[].language` stay `de`, and align (German aeneas voice) and NER (German spaCy model) are unreliable on those speeches.
- **m7k paths.** The root `https://m7k.ltsh.de/` serves no page, and the Landtag's Mediathek embeds a redesign under `/m7k-web-redesign-testversion-fa-avit/`. The root `iframe.php`, `result.php` and selector endpoints answer identically; every m7k URL derives from `M7K_BASE` in [`scraper/common.py`](scraper/common.py).
- **Aspirational `entity_dump_url`.** `de-sh.openparliament.tv` doesn't
  exist yet; the NEL stage uses the locally-built
  `metadata/entities.json` until then.
- **Per-speech video boundaries trust m7k.** The `#t=start,end` MP4
  fragment URIs come from the mediathek's own pre-computation. We do
  not verify them against the audio — the platform player honours them
  via HTML5 media-fragments.
