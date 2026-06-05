# Landtag Schleswig-Holstein (DE-SH)

Pipeline implementation for the Schleswig-Holstein Landtag mediathek
(`m7k.ltsh.de`). See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md)
for the cross-parliament onboarding flow. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two input streams *would* be the norm; for DE-SH only one is wired up:

- **Media stream** — `scraper/fetch_media.py` POSTs the m7k AJAX endpoint
  `result.php` once per Tagung (WP > Tagung > Sitzung is the source-side
  hierarchy) and saves the raw HTML response under
  `original/media/{YYYYMMDD}.html`. `parsers/media2json.py` extracts one
  `<div class="result">` per speech into intermediate `20NNN-media.json`,
  carrying `b={id}`, speaker, faction (`Gruppe`), TOP, theme, start/end
  time-of-day, duration, plus the derived `videoFileURI` with the
  `#t=start,end` media-fragment URI.
- **Proceedings stream** — *deferred*. Plenarprotokolle are PDF-only;
  there is no parser yet. `textContents` is emitted empty.

The **m7k AJAX feed is the spine** — it is already per-speech with
timestamps, IDs and speaker metadata, so no Needleman-Wunsch / fuzzy
alignment is needed. Sitting-day grouping happens in the merger.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) groups the
intermediate media records by `datum` (sitting day) and emits one Stage 2
record per speech with `textContents: []`. There is no join: the spine
already carries everything we have. The shared `classify_de_sh()` in
[`optv/shared/agenda_types.py`](../../shared/agenda_types.py) maps the
m7k `thema` to a `(nativeType, type)` pair.

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
# convenience wrapper (period=20, retry=20, download + merge + nel)
./update /path/to/OpenParliamentTV-Data-DE-SH

# explicit
./workflow.py --period=20 /path/to/OpenParliamentTV-Data-DE-SH \
    --download-original --merge-speeches --link-entities
```

`--align-sentences` and `--extract-entities` are intentionally *not*
supported — see [manifest.yaml](manifest.yaml) (`supported_stages`).
Pre-NEL the entity dump can be regenerated with:

```sh
python -m optv.parliaments.DE-SH.scraper.build_entity_dump /path/to/OpenParliamentTV-Data-DE-SH
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

- **No transcript text.** Plenarprotokolle are PDF-only and no PDF
  parser exists yet. `textContents` is empty; the platform shows the
  video clip with speaker / faction / agenda metadata only. Once a PDF
  spine is built, `align` and `ner` can be added.
- **Scope: WP 20 only.** WP 18 (from Jan 2014) and WP 19 are available
  on m7k but not yet onboarded. The scraper code is term-agnostic; only
  the manifest needs to be widened.
- **Aspirational `entity_dump_url`.** `de-sh.openparliament.tv` doesn't
  exist yet; the NEL stage uses the locally-built
  `metadata/entities.json` until then.
- **Per-speech video boundaries trust m7k.** The `#t=start,end` MP4
  fragment URIs come from the mediathek's own pre-computation. We do
  not verify them against the audio — the platform player honours them
  via HTML5 media-fragments.
