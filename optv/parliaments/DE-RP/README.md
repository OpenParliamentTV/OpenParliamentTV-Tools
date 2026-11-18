# Landtag Rheinland-Pfalz (DE-RP)

Parser/merger for the Rhineland-Palatinate state parliament (Wahlperiode 18). Mirrors the DE Bundestag's architecture (proceedings-spine + Needleman-Wunsch) but reads both streams from local inboxes rather than live feeds. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams, both delivered into local inboxes (no live scraper). Proceedings are the authoritative spine.

- **Proceedings stream** ([`scraper/ingest_xml.py`](scraper/ingest_xml.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): Dataport-generated ePlenarprotokoll ("ePP") XML dropped into `<data_dir>/inbox/`. Ingest copies into `original/proceedings/<SESSION>-proceedings.xml`; the parser produces `<SESSION>-proceedings.json`. Carries the structured `<TOP thema/>` agenda used downstream for `nativeType` / `type` classification.
- **Media stream** ([`scraper/fetch_media.py`](scraper/fetch_media.py) → [`parsers/media2json.py`](parsers/media2json.py)): OPAL "Suche nach Reden" rendered HTML in `<data_dir>/original/media/inbox/`, saved from a browser or by [`scraper/fetch_opal.py`](scraper/fetch_opal.py). Each row becomes part of `original/media/raw-<SESSION>-media.json` and then `<SESSION>-media.json`, with speaker, party or office, page range, function tag and video URL — but **no agenda title** (OPAL only carries function tags).

### OPAL rows

- **Video files** are named `{sitting}-Sit{N}{suffix}.mp4`. `N` is a running number in protocol order. A clip that follows `N` without a number of its own gets a letter suffix `a`–`d`: some Kurzinterventionen and the replies to them, statements of the presiding officer, occasionally a speech. Some numbers exist only with suffixes. Rows sort by `(N, suffix)`, which gives the `speechIndex`.
- **Electoral period** comes from the `<p>Plenarsitzung WP/SS</p>` header that precedes each sitting's rows; the file names carry none. A row before any header, or under a header whose sitting differs from the file name, is skipped with a warning, as is a row whose text does not parse.
- **`media.originMediaID`** is `{period}-{file name}`, e.g. `18-81-Sit6a`, because the same file name recurs in the next period.
- **Party or office**: the value in parentheses after the speaker is a faction (`people[].faction`) or an office (`people[].role`): chair titles, ministry codes such as `MdI`, `Ministerpräsident`, … ([`parsers/common.py`](parsers/common.py) `is_office`). A function tag (`Kurzintervention`, `Bericht`, `Bekanntgabe des Abstimmungsergebnisses`) goes to `debug.function` and, for speakers without an office, to `role`. In a merged speech the proceedings role wins.
- **Fingerprint**: with `--fingerprint-media`, ingest HEADs each video file that has no fingerprint yet (1 request/s) and records `Last-Modified` and `Content-Length` (`media.additionalInformation.lastModified` / `contentLength`). `--recheck-media` HEADs every file again and marks one whose values changed. `media2json` sets `debug.mediaReplaced` and logs an error when a file is gone, changed since it was fingerprinted, or was modified more than 30 days after the sitting.

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
#     --fingerprint-media \
#     <data_dir>
```

The session regex scopes downstream stages to the WP 18 sessions for which ePP XML has arrived (77, 78, 80–85); extra OPAL media outside that range still lands in `original/media/` but is not published. Update the regex when more proceedings arrive. `make download` / `make all` mirror the mtime-driven flow. The first `--fingerprint-media` run over an inbox HEADs every clip once (about 15 minutes for 800 clips); later runs only HEAD new clips.

For a test run, `--fetch-opal FROM TO` saves OPAL's speech search for `--period` and that date range into the media inbox before ingest (headless browser; install with `pip install -r requirements-browser.txt && scrapling install`):

```bash
python3 optv/parliaments/DE-RP/workflow.py --period=19 --fetch-opal 2026-09-16 2026-09-17 \
    --download-original <data_dir>
# or standalone:
python3 optv/parliaments/DE-RP/scraper/fetch_opal.py --wp 19 --from 2026-09-16 --to 2026-09-17 \
    --inbox-dir <data_dir>/original/media/inbox
```

## Access notes

- **ePP XML** is consumed from the inbox; no live fetch is wired up. The public XML export (`dokumente.landtag.rlp.de/landtag/xml-dokumente/`, one file per period) is flat HTML-in-XML, has no WP 19 file, and its WP 18 file ends with sitting 79.
- **OPAL** renders its results with JavaScript. A plain HTTP request to a link URL (e.g. `https://opal.rlp.de/portal/browse.tt.html?type=generic4&action=link&searchgeneric4-from=16.09.2026&searchgeneric4-to=17.09.2026&searchgeneric4-wp=19`) gets only the page frame; a browser renders the results. OPAL lists 50 hits per page; `fetch_opal.py` switches to "Alle auf einer Seite". opal.rlp.de's robots.txt allows only Googlebot, so headless fetching is for test runs.
- **Video files** on `dokumente.landtag.rlp.de/landtag/opal-videos/` are served without robots.txt; a missing file redirects (302) to the Landtag's start page. A sitting's files can be enumerated by running number plus the suffixes `a`–`d`; since a number can exist only with suffixes, a 302 for a number does not end the list.

## Known limitations

- **Not live anywhere.** No live scraper and narrow ePP coverage keep this on a session-by-session cadence rather than continuous ingestion.
- **Session scope: WP 18 / 77, 78, 80–85.** Only the sessions for which ePP XML has been delivered. Pre-WP-18 periods have not been surveyed for ePP availability.
- **No text source for WP 19.** The ePP XML covers WP 18 only and the public XML export has no WP 19 file. The protocol PDFs (`dokumente.landtag.rlp.de/landtag/plenarprotokolle/{sitting}-P-{WP}.pdf`) are the only public WP 19 text; a PDF text tier (as for the other Landtage, via `optv.shared.pdf2tei`) is not built. WP 19 OPAL media can be ingested but has nothing to merge with.
- **Video URLs are reused across electoral periods.** The server overwrites `{sitting}-Sit{N}.mp4` once the next period reaches the same sitting number, so a published `media.videoFileURI` later plays another speech. The fingerprint and `debug.mediaReplaced` detect this; no copies of the files are kept.
- **NEL.** `entity_dump_url` is **aspirational** (the `de-rp.openparliament.tv` host does not exist yet); NEL reads the local `<data_dir>/metadata/entities.json`. DE-RP has no `build_entity_dump.py` of its own. The current dump covers 857 lifetime RLP MPs + 7 faction QIDs and misses cabinet ministers without an MP Q-ID plus some MPs without `P39` statements.
- **Merger confidence tuning is conservative.** `merge_penalty` / `split_penalty` are hardcoded at `-1`; future work could tune them against media-duration / text-length ratios.
