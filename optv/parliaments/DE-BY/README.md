# Bayerischer Landtag (DE-BY)

Pipeline implementation for the Bavarian state parliament's "Plenum Online"
video archive (`www1.bayern.landtag.de/plon-webanzeige`). See
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for the
cross-parliament onboarding flow. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two input streams *would* be the norm; for DE-BY only one is wired up:

- **Media stream** — `scraper/fetch_archive.py` drives the PrimeFaces JSF
  accordion to build a session index, and `scraper/fetch_media.py` expands each
  Tagesordnungspunkt to download its per-TOP `meta_vod_*.json` playlist (one
  HLS master per speech). `parsers/media2json.py` flattens these into
  `original/media/19{NNN}-media.json`, one record per speech carrying speaker,
  party, TOP title, the HLS master URL, and the speech start time (parsed from
  the 14-digit timestamp in the HLS filename).
- **Proceedings stream** — *deferred*. Plenarprotokolle are PDF-only; there is
  no parser yet. `textContents` is emitted empty.

The **Plenum Online playlists are the spine** — already per-speech with
speaker, party and agenda title — so no Needleman-Wunsch / fuzzy alignment is
needed. The merger is a translation pass.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) emits one Stage 2 record
per speech with `textContents: []`, one `agendaItem` per TOP
(`agendaItem.id = "TOP-{index}"`, title from the accordion header). The shared
`classify_de_by()` in [`optv/shared/agenda_types.py`](../../shared/agenda_types.py)
maps the TOP title to a `(nativeType, type)` pair.

`session.number` is the citation **Sitzungsnr** (read from the loaded session's
Tagesordnung link); `electoralPeriod.number` is the Wahlperiode. The session key
is `19{NNN}` (e.g. `19054` = WP 19, 54. Sitzung). Bavaria has a pure two-level
hierarchy (Wahlperiode > Sitzung) — no Tagung/Sitzungsperiode level to drop.

## Running

```sh
# convenience wrapper (period=19, retry=20, download + merge + nel)
./update /path/to/OpenParliamentTV-Data-DE-BY

# explicit; smoke-test one session
./workflow.py --period=19 /path/to/OpenParliamentTV-Data-DE-BY \
    --download-original --merge-speeches --link-entities --limit-session '19054'
```

`--align-sentences` and `--extract-entities` are intentionally *not* supported —
see [manifest.yaml](manifest.yaml) (`supported_stages`). Pre-NEL the entity dump
can be regenerated with:

```sh
python -m optv.parliaments.DE-BY.scraper.build_entity_dump /path/to/OpenParliamentTV-Data-DE-BY
```

This writes `metadata/entities.json`; the NEL stage falls back to it when the
hosted dump at `de-by.openparliament.tv` doesn't exist (it currently doesn't).

## Access notes

The session/TOP index is a PrimeFaces 13 JSF app — there is no directory
listing or JSON index for the playlists. The scraper drives the stateful Ajax
conversation (`scraper/common.py`): GET seeds the `PLON-Webanzeige` cookie +
`_csrf` + `jakarta.faces.ViewState`; a `valueChange` POST loads a session by its
`sitzungGremiumId`; a per-TOP `tabChange` POST (with the dynamic-load params
`accordion_contentLoad` / `accordion_newTab` / `accordion_tabindex`) lazy-loads
the panel whose `openTV1OndemandWindow(...)` onclick handlers carry the
`meta_vod` URL. Two id counters are independent: the dropdown `sitzungGremiumId`
(e.g. 614 for 02.07.2025) is **not** the streaming-folder id (`19_625`), so the
playlist URLs must be scraped from the rendered panels, not constructed.

## Known limitations

- **No transcript text.** Plenarprotokolle are PDF-only and there is no parser;
  `textContents: []`, `align`/`ner` omitted. The published clips still carry
  speaker / faction / agenda metadata. (DE-SH regime — see audit §4.7.)
- **No per-speech end time.** The source gives only a start timestamp (in the
  HLS filename); `dateEnd` equals `dateStart` and `media.duration` is unset. The
  HLS master is itself the per-speech clip, so the player ends naturally.
- **Video is copyright-restricted** (Urheberrecht; reproduction needs prior
  permission). Stage 2 references the Landtag CDN HLS URL and does not rehost.
- **NEL coverage gap** (DE-SH class): Wikidata `P39 wd:Q21030144` misses current
  WP-19 members lacking that statement, so person-NEL is partial. Fixable
  downstream by enriching the SPARQL or the abgeordnetenwatch roster (id 13).
- **Scope:** WP 19 only. Older Wahlperioden are in the archive but not wired up
  (older sessions may predate the JSON-playlist format).
