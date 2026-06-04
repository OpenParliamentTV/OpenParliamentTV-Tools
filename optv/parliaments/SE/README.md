# Sveriges Riksdag (SE)

Parser/merger for the Swedish Riksdag (riksmöte 2025/26 and forward). For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md).

## Data model

Two streams, both fetched in one walk via [`scraper/fetch_session.py`](scraper/fetch_session.py), then joined per session on a clean integer key. **Media is the authoritative spine**: media-only speeches (no matching text) are kept with empty `textContents`; proceedings-only entries are dropped (the platform prefers video without text over text-only).

- **Media stream** ([`scraper/fetch_session.py`](scraper/fetch_session.py) → [`parsers/media2json.py`](parsers/media2json.py)): `dokumentstatus/{rel_dok_id}.json?utdata=debatt,media` per debate document. Each debate gives one MP4 + per-speech offsets (`startpos`, `anf_sekunder`); per-speech URLs are encoded as Media Fragment URIs (`#t=start,end`). Lands in `original/media/{session}-media.json`.
- **Proceedings stream** ([`scraper/fetch_session.py`](scraper/fetch_session.py) → [`parsers/proceedings2json.py`](parsers/proceedings2json.py)): walks `anforande/{protokoll_id}-N.json` until 3 empty responses, plus the `dokumentstatus` for protokoll metadata. Sentence segmentation via spaCy `sv_core_news_md`; lands in `original/proceedings/{session}-proceedings.json`.
- **Per-speech audio** ([`align_prep.py`](align_prep.py)): the per-debate MP3 (~40 min, 5–40 speeches) is sliced into per-speech MP3s via `ffmpeg -c copy` (keyframe-aligned, drifts ~100 ms — acceptable for aeneas).

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) joins on a single integer **`anforande_nummer`** (protokoll-wide numbering, verified clean 2026-04-30). It builds a `{anforande_nummer: speech}` index from proceedings and grafts text by exact match onto each media record. Media-only speeches surface as `debug.merge.text-missing = true`.

## Running

`--session` is required for downloads (Riksdag's `anforandelista` filter is unreliable so the wrapper can't auto-discover them), so the `update` wrapper and `Makefile` cover **post-download** stages only — merge + align + NER over whatever raw files already exist under `original/`:

```bash
./optv/parliaments/SE/update <data_dir>
# or, with finer control / to download a new protokoll:
./optv/parliaments/SE/workflow.py --period=2025 <data_dir> \
    --session HD0991 \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

`--period=2025` means riksmöte 2025/26. Session keys are `{period}-{protokoll_nr:03d}` (e.g. `2025-091`). `--limit-anforanden N` is testing-only (stop the per-anforande walk after N). `make` mirrors the wrapper.

## Known limitations

- **`--session` required for downloads.** The Riksdag `anforandelista` filter is unreliable (verified 2026-04-30); direct per-anforande lookup is the only stable path. There is no period-wide auto-discovery yet — the future approach is the bulk dataset URL pattern `data.riksdagen.se/dataset/anforande/anforande-{rm_compact}.json.zip`.
- **Zero-duration procedural speeches.** Riksdag publishes talman/vice talman procedural calls with `anf_sekunder=0` despite carrying transcript text. [`align_prep.py`](align_prep.py) skips these cleanly, sets `debug.align-skip = "zero-duration-from-source"`, and removes any stale cache files.
- **Fragment-missing fallback.** When a per-speech `#t=startpos,duration` fragment is missing, the aligner falls back to downloading the full-debate MP3 (1–2 hrs), which can deadlock Whisper QC. Production text output is unaffected.
- **NEL.** `entity_dump_url` is empty until an SE `entities.json` is hosted upstream; the NEL stage works as soon as a local `<data_dir>/metadata/entities.json` is present (current dump ≈ 2 077 Riksdag members + 8 active parties, expanded to ~5 600 aliases at load time).
- **No rollback handling.** Withdrawn or revised speeches are taken as-is from the canonical Riksdag numbering — no special-case logic in the merger.
