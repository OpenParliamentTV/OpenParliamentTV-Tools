# Niedersächsischer Landtag (DE-NI)

This directory implements the OpenParliamentTV pipeline for the Niedersächsischer Landtag (Lower Saxony, WP 18 and WP 19). See [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide context. It follows the DE-HH single-source video-spine shape, but the source is much richer: Plenar-TV (`plenartv.de`) is a SvelteKit SPA backed by a **public, unauthenticated REST API** (`api.plenartv.de`) that delivers the agenda, the per-speech speaker timings, stable speaker IDs (`abg_id`), speaker roles and, from WP 19, subtitles per agenda item. There is no scraping and no cross-source alignment. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

> On top of **video + speaker + agenda metadata**, the Plenar-TV subtitles (WP 19) are parsed (`parsers/vtt2json.py`) and attached as `textContents` (`supported_stages: [download, parse, merge, nel, ner]`). Their text is the plenary protocol nearly word for word, without interjections and without speaker labels, and their cues carry times, so sentence timings come straight from the subtitles: `align` (aeneas) is **not** needed and is omitted; only `ner` runs. **This VTT text path is experimental and unvalidated.**

## Data model

Two sources, both from the Plenar-TV REST API: the **media** spine (agenda + per-speech speaker timings) and the Plenar-TV **WebVTT subtitles** as the proceedings text. There is no fuzzy alignment: the cues are placed in the stream by the agenda item's start and split by the speech windows.

- `scraper/fetch_archive.py` → the per-Sitzung index (`metadata/archive-wp{N}.json`). Discovery walks the Tagungsabschnitt numbers via `GET /session/periode/{wp}/session/{N}` and collects every `meeting` (Sitzung) until consecutive misses; cheap public GETs.
- `scraper/fetch_media.py` → for each in-scope Sitzung, `GET /subject/date/{date}` (the agenda items / TOPs), then `GET /subject/{id}` per subject for its `speakerTimings`, and one one-segment clip playlist per stream for its HLS segment length, into `original/media/{session_id}-items.json`. Subjects are kept in agenda order (item number, subject number). Jointly debated items (`item.togetherStatus` 1/2/3 = first/middle/last) list the same speaker timings; each timing is kept once, under the first item of the debate, which lists the others in `joint_subjects`. The subtitle file of a joint debate comes from the first of its items that has one (`vtt_subject_id`).
- `parsers/media2json.py` → flattens subjects × speaker timings to `original/media/{session_id}-media.json` (one record per speech: `name`+`surname` joined to `Firstname Lastname`, `fraktion` kept as faction, `function` kept as role, speaker `context` from `function` for the chair and else from `speechType`, stream-second offsets, the clip's actual start second, real wall-clock `start`/`end`, global `speech_index` in wall-clock order).
- `parsers/vtt2json.py` → fetches the subtitles of each agenda item (`GET /vtt/{subject_id}`), places the cues in the stream as the web player does: cue time 0 is the start of the player's clip of the agenda item (`?start={itemStartTimeInStreamSecs}`, which begins up to one segment early, see below) plus the item's `vttOffset`, and gives each cue to the speech window holding its midpoint (the latest-starting one where windows overlap; cues between windows, the chair handing over, are dropped). Sentence timings count from the clip start. The merger's `attach_text_by_index` attaches these onto each speech by `speechIndex`.

API shapes used (all GET, no auth):

- `/session/periode/{wp}/session/{tagungsabschnitt}` → `{ sessionNumber, electionPeriod, meetings:[{id, meetingDate, meetingNumber}] }`
- `/subject/date/{meetingDate}` → the Sitzung's subjects (without timings)
- `/subject/{subject_id}` → one subject **with** `speakerTimings:[{id, abg_id, surname, name, fraktion, function, speechType, startTimeInStreamSecs, stopTimeInStreamSecs}]`, plus `itemStartTimeInStreamSecs` / `itemStopTimeInStreamSecs`, `streamFileName`, `video.startTime` (WP 19), `vttFile`, `vttOffset` and `item.{itemNumber, itemBeginning, togetherStatus}`
- `/vtt/{subject_id}` → `text/vtt`; the web player plays it with the clip of the agenda item and adds `vttOffset` to every cue (the offset makes up for the clip's early start and, for some items, for a shifted subtitle file)

Per-speech video is a **server-side-clipped HLS playlist**, addressed directly: `https://vod.plenartv.de/stream/{streamFileName}/index.m3u8?start={sec}&end={sec}` (no `#t=start,end` fragment). The server returns whole HLS segments, `ceil(start/d) − 1` to `ceil(end/d) − 1` (0-based, `d` = the stream's segment length, 1 s or 10 s), so a clip starts up to one segment before the speech; `media.additionalInformation.clipStartInStreamSecs` is that second, and sentence timings count from it. Per-speech wall-clock is `video.startTime` (UTC) + the stream-second offset.

## Merge strategy

No cross-source matching: `merger/merge_session.py` is a single-source translation pass into Stage 2 (the API stream is the authoritative spine). One `agendaItem` per subject (`id = "TOP-{item}-{subjectNumber}"`, classified via `optv.shared.agenda_types.classify_de_ni` over `title`+`subjectArt`+`consultationType`), one `people[]` entry per speech (`abg_id` carried as `originPersonID`, `function` as `role`), and media as the per-speech HLS clip. A speech of a joint debate carries the first item of the debate as `agendaItem` and the others in `debug.jointAgendaItems`. `sourcePage` is the Plenar-TV player page of the speech's clip range (`https://plenartv.de/player?file={streamFileName}&startPos={s}&endPos={e}`), unique per speech (the platform keys speech identity on it); the Sitzung page is `meta.sourceURI`. `originID` is the source-native speakerTiming UUID. Speech `dateStart`/`dateEnd` are **real wall-clock** UTC, so `debug.timesAreVideoRelative = false`.

## Running

```sh
# Routine run (newest period): download, merge, link entities
./workflow.py /path/to/OpenParliamentTV-Data-DE-NI \
    --download-original --merge-speeches --link-entities

# Pre-NEL, (re)generate the local entity dump (Wikidata P39 Q17521638 + WP-18/19 parties):
python -m optv.parliaments.DE-NI.scraper.build_entity_dump /path/to/OpenParliamentTV-Data-DE-NI

# Explicit, single test session (WP19 Sitzung 80, 16 Dec 2025):
./workflow.py --period=19 /path/to/OpenParliamentTV-Data-DE-NI \
    --download-original --merge-speeches --link-entities \
    --limit-session '19080'

# WP 18 (video + speakers, no subtitles), e.g. Sitzung 9, 28 Feb 2018:
./workflow.py --period=18 /path/to/OpenParliamentTV-Data-DE-NI \
    --download-original --merge-speeches --link-entities \
    --limit-session '18009'

# Validate the published output:
python -m optv.shared.validators.cli --dir /path/to/OpenParliamentTV-Data-DE-NI/processed --schema full
```

Session discovery enumerates Tagungsabschnitte 1..N (stop after consecutive misses; `--max-tagungsabschnitt` bounds it) and caches the result in `archive-wp{N}.json`. The Plenar-TV "session" is a Tagungsabschnitt (a multi-day block); a "meeting" is a single Sitzung — OPTV's `session.number` is the Sitzung.

## Access notes

The REST API at `api.plenartv.de` is public and needs no token for reads (the `auth.plenartv.de` token flow is only for VTT upload/admin). The SvelteKit web app is a thin client over it; we never touch the HTML. Same vendor lineage as the Hamburg mediathek, but DE-NI exposes a clean typed JSON API rather than static markup.

## Known limitations

- **Experimental, unvalidated VTT text path.** Text comes from the Plenar-TV subtitles per agenda item (`GET /vtt/{subject_id}`), parsed by `parsers/vtt2json.py` and attached as `textContents` with cue-derived sentence timings (so `align`/aeneas is not needed; only `ner` runs). The subtitles carry no speaker labels, so the chair's remarks inside a speech window stay in that speech's text, attributed to the speaker, and the speech windows of the API are cut by hand, so a speech's first or last sentence can fall outside its window. **Not ready for platform integration.** The verbatim Plenarprotokoll PDF (with interjections) is an alternative text source not used here; the merger records the subtitle URL in `media.additionalInformation.subtitleVttURI`.
- **Joint debates have one agenda item.** A speech in a joint debate carries the first item of the debate as `agendaItem` and the others only in `debug.jointAgendaItems`, until the data model can hold several agenda items per speech.
- **Clips start early.** The VOD server rounds a clip out to whole HLS segments, so a clip can start up to one segment (1 s or 10 s, depending on the stream) before the speech and end up to one segment after it.
- **NEL coverage caveat** (DE-HH class): the entity dump is built from Wikidata `P39 wd:Q17521638` ("member of the Landtag of Lower Saxony"); it misses members lacking that statement and government members (Ministerinnen/Minister) who are not sitting MdL. The API exposes a stable `abg_id` per speaker — a future `abg_id`→Wikidata bridge would close the residual gap; v1 stays name-based.
- **Government members' faction varies.** Some ministers carry their party in `fraktion`, others none; those speeches have no `faction` and trigger benign `semantic.people.faction.missing` warnings.
- **WP 18 wall-clock is estimated.** WP 18 streams have no `video.startTime`; the stream start is estimated from the agenda items' local `itemBeginning`, which is given to the minute (`debug.streamStartEstimated`), so `dateStart`/`dateEnd` are accurate to about a minute. WP 18 has no subtitles. Some WP 18 subjects name a stream without its `_std` suffix that does not exist on the VOD server; the scraper points them at the `_std` stream of the same part.
- **WP 17 is not wired.** The API has speech timings from Tagungsabschnitt 12 of WP 17 (2014-02) on, without speaker IDs.
