# Landtag Rheinland-Pfalz (DE-RP)

Parser/merger for the Rhineland-Palatinate state parliament (Wahlperiode 18). Mirrors the DE Bundestag's architecture (proceedings-spine + Needleman-Wunsch) but reads both streams from local inboxes rather than live feeds. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md).

## Data model

Two streams, both delivered into local inboxes (no live scraper). Proceedings are the authoritative spine.

- **Proceedings stream** ([`scraper/ingest_xml.py`](scraper/ingest_xml.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): Dataport-generated ePlenarprotokoll ("ePP") XML dropped into `<data_dir>/inbox/`. Ingest copies into `original/proceedings/<SESSION>-proceedings.xml`; the parser produces `<SESSION>-proceedings.json`. Carries the structured `<TOP thema/>` agenda used downstream for `nativeType` / `type` classification.
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): OPAL "Suche nach Reden" rendered HTML, hand-saved from a browser into `<data_dir>/original/media/inbox/`. The parser turns each row into `original/media/<SESSION>-media.json` with speaker, party, page anchors and video URL — but **no agenda title** (OPAL only carries function tags).

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) runs **Needleman-Wunsch alignment** on speaker name + agenda title (weights 4/2). Speaker comparison strips honorifics and de-accents (`speaker_cleanup`). Confidence: full match `1.0`; speaker mismatch halves; title mismatch × 0.9. The agenda title is carried from the ePP proceedings (OPAL has none), so merged items inherit `nativeType` / `type` from the XML.

## Running

```bash
./optv/parliaments/DE-RP/update <data_dir>
# expands to:
# python3 workflow.py --period=18 \
#     --limit-session '1807[7-9]|1808[0-5]' \
#     --inbox-dir <data_dir>/inbox \
#     --media-inbox-dir <data_dir>/original/media/inbox \
#     <data_dir>
```

The session regex scopes downstream stages to the WP 18 sessions for which ePP XML has actually arrived (currently 77–85); extra OPAL media outside that range still lands in `original/media/` but is not published. Update the regex when more proceedings arrive. `make download` / `make all` mirror the mtime-driven flow.

## Access notes

There is no live scraper for either stream:

- **ePP XML** is consumed from the inbox; no live fetch is wired up. The public WP XML is flat/unstructured and is only a fallback.
- **OPAL** is a JavaScript SPA — deep-link URLs redirect to the shell. Per-speech video listings have to be hand-saved as rendered HTML or fetched via a headless browser.

## Known limitations

- **Not live anywhere.** No live scraper, narrow ePP coverage, and an imminent WP 19 election keep this on a session-by-session cadence rather than continuous ingestion.
- **Session scope: WP 18 / 77–85.** Only the sessions for which ePP XML has been delivered. Pre-WP-18 periods have not been surveyed for ePP availability.
- **WP 19 transition risk (2026-03-22).** When WP 19 starts the Landtag portal and the XML format may change — both [`scraper/fetch_media.py`](scraper/fetch_media.py) (OPAL HTML scraping) and [`scraper/ingest_xml.py`](scraper/ingest_xml.py) may need rework.
- **NEL.** `entity_dump_url` is **aspirational** (the `de-rp.openparliament.tv` host does not exist yet); until the dump is hosted, point `--nel-data-dir` at a local `<data_dir>/metadata/entities.json` built via `scraper/build_entity_dump.py`. The current dump covers 857 lifetime RLP MPs + 7 faction QIDs and misses cabinet ministers without an MP Q-ID plus some MPs without `P39` statements.
- **Merger confidence tuning is conservative.** `merge_penalty` / `split_penalty` are hardcoded at `-1`; future work could tune them against media-duration / text-length ratios.
