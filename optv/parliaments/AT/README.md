# Österreichischer Nationalrat (AT)

This directory implements the Open Parliament TV pipeline for the Austrian National Council (Nationalrat). See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide context and [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md) for how AT's data shape fits the cross-parliament model.

## Data model

Both input streams come from parlament.gv.at's documented `?json=true` Open Data endpoints (case-sensitive — lowercase; `?json=TRUE` returns the site's SvelteKit HTML shell). The only internal dependency is the `AcquireContent` video-URL resolver.

- **Media (the spine)** — `scraper/fetch_session.py` fetches the Mediathek detail page's documented Open Data JSON, `/aktuelles/mediathek/{GP}/NRSITZ/{n}?json=true` (lowercase — `?json=TRUE` returns the SvelteKit HTML shell). Its `content.mediumdata.debatten[].redner[]` is the canonical list of on-camera speeches; each `redner` carries `std_id` (the join key), `uuid`/`ts` (per-speech video clip), `name`, `pad_intern` (person id) and `protokoll` (the per-speech stenographic-protocol HTML). Each clip is resolved to HLS/MP4/MP3 URLs through the `AcquireContent` video API. `parsers/media2json.py` turns this into per-speech media records.
- **Proceedings** — the per-speech protocol HTML (one Word-generated file per `std_id`) is fetched alongside the payload into `original/proceedings/{session}/{std_id}.html` and parsed by `parsers/proceedings2json.py` into per-speech text, speakers and factions.

On-disk: `original/media/{session}-mediathek.json` (raw spine) and `original/proceedings/{session}/*.html` (raw text); parsed intermediates are `{session}-media.json` / `{session}-proceedings.json`. Session keys are `{period}{sitting:03d}` (e.g. `27144`).

## Merge strategy

`merger/merge_session.py` is a media-spine merge with an **exact id join**: the protocol `std_id` equals the media `st_objekte_id`, so transcript text grafts onto each video clip by integer key — no fuzzy/Needleman-Wunsch alignment. The on-camera speaker is identified by `pad_intern` (matching the protocol header's `PAD_<n>` anchor) and guaranteed present and first in `people`. Because that one `std_id` is *both* the media and text source id, it is written to `media.originMediaID` and `textContents[].originTextID` and `speech.originID` is left unset. Speeches without matching text are kept media-only.

## Running

```bash
# Download specific sittings, parse, merge, link, align, extract entities:
./optv/parliaments/AT/workflow.py --period=27 <data_dir> \
    --download-original --sitting 144 --sitting 200 --sitting 257 \
    --merge-speeches --link-entities --align-sentences --extract-entities \
    --ner-api-endpoint http://localhost:8090/service

# Backfill the whole period (omit --sitting to discover sittings by walking
# the Mediathek pages):
./optv/parliaments/AT/workflow.py --period=27 <data_dir> --download-original --merge-speeches
```

`--sitting N` (repeatable) scopes the download; omit it to auto-discover the period. `--limit-session <regex>` scopes the merge/align/NER stages. Build the NEL entity dump once with `python -m optv.parliaments.AT.scraper.build_entities <data_dir>` (writes `metadata/entities.json`), then run `--link-entities`.

## Access notes

All sources are plain public HTTP — no auth, no WAF bypass. The spine and proceedings come from the documented `?json=true` Open Data endpoints. The one undocumented/internal dependency is the video resolver, a third-party host (`api.ausp.cloud.insysgo.com`) that turns a clip's `uuid`/`ts` into HLS/MP4/MP3 URLs — the same call the site itself makes.

## Known limitations

- **Scope:** electoral period 27 (XXVII. GP) only — the period covered by the test corpus. Other periods need their GP code (auto-derived) and a re-run; pre-July-2019 sittings have no per-speech video.
- **Speaker Wikidata coverage is ~97%.** The entity dump (`build_entities.py`) covers the 200 members of the 27th-term National Council, 23 curated federal government members (ministers/chancellors who aren't MdNs), and the 5 parliamentary clubs. The remaining gap is lesser officials (state secretaries, the Rechnungshof president), a few ministers outside the curated list, and a handful of MPs whose protocol name differs from their Wikidata label — surfaced as `semantic.people.wid.missing`. Ministers also (correctly) carry no faction, so they raise `semantic.people.faction.missing` despite being valid; both are warnings, never errors.
- **Alignment audio is the server-trimmed HLS window** (`media.videoFileURI` carries `?startseconds=…&stopseconds=…`, and the stream server returns exactly that window). `align_prep.py` transcodes it per speech; the per-speech MP3/MP4 clip assets the resolver also returns are unreliable (sometimes absent, sometimes the whole session) and are not used for alignment. Clips longer than 40 min (whole-session/procedural containers) are skipped (`debug.alignSkip`). A single clip's transcode failure is logged and skipped, never aborting the session. Alignment quality is treated as provisional until the Whisper-QC text-fidelity audit clears it.
- **Discovery walk:** the period backfill probes sitting numbers sequentially and stops after several consecutive empty Mediathek pages; a long gap in numbering would cut it short (not observed in EP27, which is contiguous).
