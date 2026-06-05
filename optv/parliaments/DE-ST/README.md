# Landtag von Sachsen-Anhalt (DE-ST)

Parser/merger for the Saxony-Anhalt state parliament (Wahlperiode 8, since 2021). Both proceedings and media come from the same Landtag portal (`landtag.sachsen-anhalt.de`); the per-speech text-video join is in the DOM, so no Needleman-Wunsch alignment is needed. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

A single source page per **Sitzungsperiode** (`/{N}-sitzungsperiode`) bundles 1–3 **Landtagssitzungen** (calendar days). The portal exposes the Sitzungsperiode as the URL boundary; we treat each Sitzung as the OPTV `session` (matching DE/DE-RP's convention). One scraped HTML page therefore yields N intermediate proceedings files and N media file collections — one per day-section (`<section id="section-N">`).

- **Archive crawler** ([`scraper/fetch_archive.py`](scraper/fetch_archive.py)): walks `/archiv`, fetches each `sp-{NN:03d}.html`, counts day-sections, and builds a cumulative Sitzung map written to `<data>/metadata/sitzung-map.json`. The canonical Sitzung number is not exposed as structured data anywhere on the portal — we derive it from the cumulative day-count and correct for any archive gaps via a single transcript probe on the latest SP (see "Sitzung numbering" below).
- **Per-speech video** ([`scraper/fetch_media.py`](scraper/fetch_media.py)): for every standard video player-id (`data-js-id="video-std"`; sign-language `video-sign` skipped), GETs `?videoSessions=videoAjax&videoId={pid}` and saves the raw HTML under `original/media/{08NNN}/{player-id}.html`. The AJAX response carries a `data-jsb` JSON blob with the MP4 URLs (1080p/720p/360p), duration, and thumbnail.
- **Per-Sitzung proceedings** ([`parsers/proceedings2json.py`](parsers/proceedings2json.py)): splits the SP HTML by day-section and emits `{08NNN}-proceedings.json` for each Sitzung. Each speech record carries speaker label, party, role, TOP number + title, std/sign player-ids, transcript speaker-id + cHash.
- **Per-Sitzung media** ([`parsers/media2json.py`](parsers/media2json.py)): decodes the `data-jsb` JSON on each AJAX response and aggregates `{08NNN}-media.json` keyed by player-id (duration, thumbnail, MP4 URLs at three qualities).
- **Transcript fetch** is deferred to the merger to keep the proceedings parser cheap and pure; cached under `original/proceedings/transcripts/{08NNN}/{speaker-id}.html`.

## Sitzung numbering

The portal URL-groups plenary work by **Sitzungsperiode** (multi-day blocks) but never renders the canonical **Landtagssitzung** number anywhere in structured HTML — only in transcript prose ("Hiermit eröffne ich die NNN. Sitzung") and the Tagesordnung / Plenarprotokoll PDF headers. We derive it structurally:

1. Walk `/archiv` → list of SP URLs in WP order.
2. For each SP HTML, count `<section id="section-N">` tags → days in that SP.
3. Cumulative sum across SPs → "expected" Sitzung number for each day.
4. **Offset probe**: fetch the latest SP's opening transcript, regex `(\d+)\. Sitzung` from the prose, compare to the cumulative count, derive offset (= number of sittings in SPs missing from the archive).
5. Apply the offset uniformly to all sittings in the map and persist.

For WP 8 this offset is `+2` because SP 42 is missing from the archive (presumably 2 sittings) — the probe self-corrects without any per-SP special-casing.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) does a **1:1 ordered zip** on the standard-video player-id (no Needleman-Wunsch). Every video-list-item on the SP page produces one entry on both streams; their order in DOM is the join order. The merger fetches each speech's transcript inline, drops the speaker prefix line, and splits the rest into speech / `(comment)` paragraphs. Per-speech `dateStart` is synthesised from cumulative durations (real per-sentence timing is filled by the alignment stage).

Agenda-item classification (TOP titles → `nativeType` / `type`) uses [`classify_de_st`](../../shared/agenda_types.py) — `Wahl` → election, `Vereidigung` → oath, `Eröffnung` → opening, `Erste/Zweite/Dritte Beratung` → regular, `Aktuelle Debatte` → current_affairs, `Befragung der Landesregierung` → questioning_of_the_government.

## Entity dump

[`scraper/build_entity_dump.py`](scraper/build_entity_dump.py) queries Wikidata for everyone with `P39 wd:Q18559580` ("member of the Landtag of Saxony-Anhalt") and combines with the hand-curated WP 8 party list. Writes `<data>/metadata/entities.json` in the format `optv.shared.nel.get_nel_data` expects. The `entity_dump_url` in [`manifest.yaml`](manifest.yaml) is aspirational; until the hosted dump exists, the workflow falls back to the local file.

## Running

```bash
./optv/parliaments/DE-ST/update <data_dir>
# expands to:
# python3 workflow.py --period=8 --retry-count=20 \
#     --limit-session '0810[56]' \
#     --download-original --merge-speeches \
#     --link-entities --align-sentences --extract-entities \
#     <data_dir>
```

The default session regex scopes downstream stages to the validated sample (Sittings 105+106 = Sitzungsperiode 47). Override with `--limit-session` to widen. `make download` / `make all` mirror the mtime-driven flow.

## Access notes

The portal is plain nginx + TYPO3 with no anti-bot — stdlib `urllib` with a polite User-Agent suffices. The video AJAX endpoint and the transcript AJAX endpoint are both undocumented (not a stable API) but have been stable across the WP 8 lifetime. **nginx rate-limits at ~60 requests / 60 s window per IP**; the scraper enforces a 1.5 s global throttle (`POLITE_DELAY` in [`scraper/common.py`](scraper/common.py)) to stay under it. A 241-speech Sitzungsperiode merge takes ~6 minutes at this rate.

## Known limitations

- **Cumulative day-count is uniformly offset.** The probe only fires for the latest SP, so the +2 offset (caused by SP 42's absence) is applied to *all* sittings — including those before SP 42. Sittings 1–N before the gap therefore appear in the map as 3–(N+2). For our immediate scope (current Sittings) this is harmless; backfilling pre-gap data would need a piecewise offset (probe each "block" between gaps).
- **`entity_dump_url` is aspirational.** The hosted `de-st.openparliament.tv/data/entity-dump/` does not exist yet. Until it does, point `--nel-data-dir` at `<data>/metadata/entities.json` produced by `scraper/build_entity_dump.py`. Coverage is ~389 MP QIDs from Wikidata plus 6 faction QIDs; non-MP speakers (Landesminister, guests) won't link.
- **Sitzungsperiode dropped from Stage 2.** The portal's URL-grouping unit is recorded only in `debug.sitzungsperiode` and `meta.sitzungsperiode`. DE-ST is another instance of the three-level temporal pattern (alongside NO/SE) that the current schema flattens.
- **No live API contract.** Video AJAX (`?videoSessions=videoAjax`) and transcript AJAX (`?transcriptSessions=lsaSessionsAjax`) are undocumented TYPO3 extension endpoints. If the portal is rebuilt, both scrapers will need rework.
- **VMS MP4 URLs may rotate.** Hash-named MP4 URLs on `app.vms.landtag-lsa.de` are persisted at scrape time; if the Landtag rotates them, alignment will need to refetch via the AJAX endpoint.
- **Transcript is Schriftdolmetschung** (live text transcription), not the official Plenarprotokoll. Wording may differ slightly from the canonical PDF in PADOKA.
