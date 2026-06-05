# Niedersächsischer Landtag (DE-NI)

This directory implements the OpenParliamentTV pipeline for the Niedersächsischer
Landtag (Lower Saxony, WP 19). See
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context. It follows the DE-HH single-source video-spine shape, but the source is
much richer: Plenar-TV (`plenartv.de`) is a SvelteKit SPA backed by a **public,
unauthenticated REST API** (`api.plenartv.de`) that delivers the agenda, the
per-speech speaker timings, stable speaker IDs (`abg_id`) and time-aligned WebVTT
subtitles. There is no scraping and no cross-source alignment. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

> The v1 onboarding ships **video + speaker + agenda metadata** with
> `textContents: []` (`supported_stages: [download, parse, merge, nel]`;
> `align`/`ner` omitted). Unlike DE-HH/DE-SH/DE-BW the text is *not* PDF-locked —
> VTT subtitles are available via the API and are the obvious next pass.

## Data model

One input stream: **media** (the Plenar-TV REST API). No proceedings stream is
merged in v1.

- `scraper/fetch_archive.py` → the per-Sitzung index
  (`metadata/archive-wp{N}.json`). Discovery walks the Tagungsabschnitt numbers
  via `GET /session/periode/{wp}/session/{N}` and collects every `meeting`
  (Sitzung) until consecutive misses; cheap public GETs.
- `scraper/fetch_media.py` → for each in-scope Sitzung, `GET /subject/date/{date}`
  (the agenda items / TOPs) then `GET /subject/{id}` per subject for its
  `speakerTimings`, into `original/media/{session_id}-items.json`.
- `parsers/media2json.py` → flattens subjects × speaker timings to
  `original/media/{session_id}-media.json` (one record per speech: `name`+`surname`
  joined to `Firstname Lastname`, `fraktion` kept as faction, speaker `context`
  derived from `speechType`, stream-second offsets, real wall-clock `start`/`end`).

API shapes used (all GET, no auth):

- `/session/periode/{wp}/session/{tagungsabschnitt}` → `{ sessionNumber, electionPeriod, meetings:[{id, meetingDate, meetingNumber}] }`
- `/subject/date/{meetingDate}` → the Sitzung's subjects (without timings)
- `/subject/{subject_id}` → one subject **with** `speakerTimings:[{abg_id, surname, name, fraktion, speechType, startTimeInStreamSecs, stopTimeInStreamSecs}]`, plus `streamFileName` and `video.startTime`
- `/vtt/{subject_id}` → `text/vtt` (not wired in v1)

Per-speech video is a **server-side-clipped HLS playlist**, addressed directly:
`https://vod.plenartv.de/stream/{streamFileName}/index.m3u8?start={sec}&end={sec}`
(the clip URL *is* the speech — no `#t=start,end` fragment needed). Per-speech
wall-clock is `video.startTime` (UTC) + the stream-second offset.

## Merge strategy

No cross-source matching: `merger/merge_session.py` is a single-source
translation pass into Stage 2 (the API stream is the authoritative spine). One
`agendaItem` per subject (`id = "TOP-{item}-{subjectNumber}"`, classified via
`optv.shared.agenda_types.classify_de_ni` over `title`+`subjectArt`+
`consultationType`), one `people[]` entry per speech (`abg_id` carried as
`originPersonID`), and media as the per-speech HLS clip. `sourcePage` is made
unique per speech via a `#rede-{timingId}` anchor (the platform keys speech
identity on it); `originID` is the source-native speakerTiming UUID. Speech
`dateStart`/`dateEnd` are **real wall-clock** UTC, so
`debug.timesAreVideoRelative = false`.

## Running

```sh
# Convenience wrapper (period 19 baked in)
./update /path/to/OpenParliamentTV-Data-DE-NI

# Pre-NEL, (re)generate the local entity dump (Wikidata P39 Q17521638 + WP-19 parties):
python -m optv.parliaments.DE-NI.scraper.build_entity_dump /path/to/OpenParliamentTV-Data-DE-NI

# Explicit, single test session (WP19 Sitzung 80, 16 Dec 2025):
./workflow.py --period=19 /path/to/OpenParliamentTV-Data-DE-NI \
    --download-original --merge-speeches --link-entities \
    --limit-session '19080'

# Validate the published output:
python -m optv.shared.validators.cli --dir /path/to/OpenParliamentTV-Data-DE-NI/processed --schema full
```

Session discovery enumerates Tagungsabschnitte 1..N (stop after consecutive
misses; `--max-tagungsabschnitt` bounds it) and caches the result in
`archive-wp{N}.json`. The Plenar-TV "session" is a Tagungsabschnitt (a multi-day
block); a "meeting" is a single Sitzung — OPTV's `session.number` is the Sitzung.

## Access notes

The REST API at `api.plenartv.de` is public and needs no token for reads (the
`auth.plenartv.de` token flow is only for VTT upload/admin). The SvelteKit web
app is a thin client over it; we never touch the HTML. Same vendor lineage as
the Hamburg mediathek, but DE-NI exposes a clean typed JSON API rather than
static markup.

## Known limitations

- **No transcript text in v1.** Ships `textContents: []`, no `align`/`ner`. The
  source *does* expose machine-readable text — time-aligned WebVTT per subject
  (`GET /vtt/{subject_id}`) and the verbatim Plenarprotokoll PDF — so a future
  pass can attach text (and, from VTT, sentence timings without aeneas). The
  merger already records the VTT URL in `media.additionalInformation.subtitleVttURI`.
- **NEL coverage caveat** (DE-HH class): the entity dump is built from Wikidata
  `P39 wd:Q17521638` ("member of the Landtag of Lower Saxony"); it misses current
  WP-19 members lacking that statement and government members (Ministerinnen/
  Minister) who are not sitting MdL. On the 19080 smoke test person-NEL is 99/106
  and faction-NEL 93/106. The API exposes a stable `abg_id` per speaker — a future
  `abg_id`→Wikidata bridge would close the residual gap; v1 stays name-based.
- **Government speakers carry no faction.** Ministers' `fraktion` is empty in the
  API (they speak as government), so those speeches have no `faction` and trigger
  benign `semantic.people.faction.missing` warnings. The chair role is signalled
  by `speechType` (`Mitteilungen`), not a role string, so chair turns map to
  `context = president` generically (president vs. vice-president is not
  distinguished per speech).
- **Wall-clock assumes `video.startTime` is UTC.** The API's stream `startTime`
  is a naive ISO timestamp; the best evidence (session opening times) indicates
  UTC, which the parser assumes. Offsets are continuous recording seconds, so the
  emitted `dateStart`/`dateEnd` may drift from the human-entered `itemBeginning`
  across breaks — the video clip boundaries are authoritative.
- **WP 19 only** (the current term). The API covers earlier periods (meeting
  numbers repeat per period, so key on `id`/date, not `meeting/number`).
