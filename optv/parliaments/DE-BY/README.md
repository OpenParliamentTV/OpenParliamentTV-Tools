# Bayerischer Landtag (DE-BY)

Pipeline implementation for the Bavarian state parliament's "Plenum Online" video archive (`www.bayern.landtag.de/plon2-webanzeige`). See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for the cross-parliament onboarding flow. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two input streams:

- **Media stream** — `scraper/fetch_archive.py` drives the PrimeFaces JSF accordion to build a session index, and `scraper/fetch_media.py` expands each Tagesordnungspunkt to download its playlists (one HLS master per speech) and the speech rows of the panel. `parsers/media2json.py` flattens these into `original/media/19{NNN}-media.json`, one record per speech carrying speaker, party, member id, TOP title, the HLS master URL, and the speech start time (parsed from the 14-digit UTC timestamp in the HLS filename and converted to Europe/Berlin).
- **Proceedings stream** — the Plenarprotokoll PDF is parsed via `optv.shared.pdf2tei` and joined onto the spine in the merger (`join_text_to_spine`). `align` (aeneas, against each speech's own HLS clip) and `ner` run on the joined text (experimental — see Known limitations).

The **Plenum Online playlists are the spine** — already per-speech with speaker, party and agenda title — so no Needleman-Wunsch / fuzzy alignment is needed for the spine; the PDF→TEI text is joined onto it.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) emits one Stage 2 record per speech and joins the PDF→TEI text onto it (`join_text_to_spine`). The join matches speakers by member id where both sides have one — the protocol links each speaker label to the member's page, the overview each speech row (`ParliamentConfig.person_link`) — and by surname otherwise (ministers have no id in the overview). Agenda items follow the playlists: a TOP with one playlist is one `agendaItem` (`id = "TOP-{index}"`, title from the accordion header); a TOP with several playlists — the debate on urgent motions (Dringlichkeitsanträge), one playlist per motion — is one `agendaItem` per motion (`id = "TOP-{index}-{k}"`, `title` = the motion title from the playlist, `officialTitle` = header + motion title). The shared `classify_de_by()` in [`optv/shared/agenda_types.py`](../../shared/agenda_types.py) maps the TOP header to a `(nativeType, type)` pair.

`session.number` is the citation **Sitzungsnr**, read from the loaded session's heading "Sitzungsablauf für die N. Plenarsitzung" (the Tagesordnung link in the same view carries the number of the plenary week's first day and is not used); `electoralPeriod.number` is the Wahlperiode. The session key is `19{NNN}` (e.g. `19054` = WP 19, 54. Sitzung). Bavaria has a pure two-level hierarchy (Wahlperiode > Sitzung) — no Tagung/Sitzungsperiode level to drop.

`originID` is `{playlistId}_{startId}`; `people[].originPersonID` is the member id from the speaker's biography link (`…/abgeordnete/abgeordnete-von-a-z/profil/{ID}`; none for state ministers). `dateStart` is wall-clock time in Europe/Berlin with its offset (`debug.timesAreVideoRelative = false`).

## Running

```sh
# Routine run (newest period): download, merge, link entities
./workflow.py /path/to/OpenParliamentTV-Data-DE-BY \
    --download-original --merge-speeches --link-entities

# explicit; smoke-test one session
./workflow.py --period=19 /path/to/OpenParliamentTV-Data-DE-BY \
    --download-original --merge-speeches --link-entities --limit-session '19054'
```

`--align-sentences` and `--extract-entities` are supported (see [manifest.yaml](manifest.yaml) `supported_stages`) but left out of the routine run while the text path is experimental. Pre-NEL the entity dump can be regenerated with:

```sh
python -m optv.shared.entity_dump_bootstrap DE-BY /path/to/OpenParliamentTV-Data-DE-BY
```

This writes `metadata/entities.json`; the NEL stage falls back to it when the hosted dump at `de-by.openparliament.tv` doesn't exist (it currently doesn't).

## Access notes

The session/TOP index is a PrimeFaces JSF app — there is no directory listing or JSON index for the playlists. The scraper drives the stateful Ajax conversation (`scraper/common.py`): GET seeds the session cookie + `_csrf` + `jakarta.faces.ViewState`; a `valueChange` POST loads a session by its `sitzungGremiumId`; a per-TOP `tabChange` POST (with the dynamic-load params `accordion_contentLoad` / `accordion_newTab` / `accordion_tabindex`) lazy-loads the panel. Its `openTV1OndemandWindow…(playerUrl, playlistUrl, startId)` onclick handlers carry the playlist URL: up to sitting 87 `…ForStreamingInfoFromMetafile` with a `/streamingservice/jsonmetafiles/wp19/…/meta_vod_{N}.json` file, from sitting 88 `…ForStreamingInfoFromDatabase` with `/plon2-webanzeige/streaming?topId={N}` (the `&gesamt=true` variant, the whole item as one stream, is skipped). Both return the same `items[]` JSON, and `N` is the same TOP-item id, so the playlist id stays continuous across the change. Each speech row in the panel carries the speaker's biography link and a download button with the speech's `aktionId`. The dropdown `sitzungGremiumId` is unrelated to the playlist ids, so the playlist URLs must be scraped from the rendered panels, not constructed.

The session index (`metadata/archive-wp19.json`) is incremental: each run reads the dropdown and loads only new sittings (plus, under `--force`, the cached sittings matching `--limit-session`). The protocol PDF is fetched in the same run as a sitting's video: `scraper/fetch_proceedings.py` takes session ids and dates from the TOP manifests, since the `-media.json` files only follow in the parse stage.

## Known limitations

- **Experimental, unvalidated text path.** The Plenarprotokoll PDF is parsed via `optv.shared.pdf2tei` and joined onto the spine, and `align`/`ner` run on the result (DE-BY is the pilot wired end-to-end). None of this has been validated — there is no Whisper-QC/text-fidelity audit yet, and the PDF→TEI extraction and the text↔spine join still need refinement. **Not ready for platform integration.**
- **No per-speech end time.** The source gives only a start timestamp (in the HLS filename); `dateEnd` equals `dateStart` and `media.duration` is unset. The HLS master is itself the per-speech clip, so the player ends naturally.
- **Video is copyright-restricted** (Urheberrecht; reproduction needs prior permission). Stage 2 references the Landtag CDN HLS URL and does not rehost.
- **NEL coverage gap** (DE-SH class): Wikidata `P39 wd:Q17586301` misses current WP-19 members lacking that statement, so person-NEL is partial. Fixable downstream by enriching the SPARQL or the abgeordnetenwatch roster (id 13).
- **Scope:** WP 19 only. Older Wahlperioden are in the archive but not wired up (older sessions may predate the JSON-playlist format).
