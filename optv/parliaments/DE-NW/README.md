# Landtag Nordrhein-Westfalen (DE-NW)

This directory implements the OpenParliamentTV pipeline for the Landtag Nordrhein-Westfalen (WP 18). It is built on a per-speech video spine; the HTML version of the Plenarprotokoll is joined onto that spine via an **experimental, unvalidated** text path (see below) — see [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide context and the DE-HH / DE-NI / DE-BW READMEs for the same regime. It combines the richest signals of that family: a parliament-native MdL id per speech, real per-speech wall-clock, and precise (second-resolution) per-speech offsets. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two input streams: **media** (the `www.landtag.nrw.de` Mediathek session pages), which is the spine, and **proceedings** (the HTML Plenarprotokoll `MMP18-{N}.html`), which is joined onto it in the merger, so matched speeches carry verbatim `textContents` (unmatched keep `[]`) and `supported_stages` includes `align`/`ner` (see Known limitations).

- `scraper/fetch_archive.py` → the candidate session index (`metadata/archive-wp{N}.json`). Sessions are addressed by an opaque `kid` (session UUID), so discovery walks the archive (`…/archivierte-aufzeichnungen.html?art=plenarsitzung&page=N`, newest first) from page 1 on every run and merges new cards into the cached index. Each card gives the `kid`, the date and the Sitzung number. Cards before the scope start (the WP start, but not before the speech list starts on 2024-06-12) are skipped, and the walk stops after the first page that holds only such cards. A `--kid` / `--session` seed records a session directly and limits the run to it, skipping pagination.
- `scraper/fetch_media.py` → fetches each session video page (`/home/mediathek/video.html?kid={kid}`), parses its static `TEST-REDNER` markup (`scraper/common.py:parse_video_page`), and derives the authoritative Sitzung number from the page's own `<h2>{N}. Plenarsitzung</h2>` header. Then, per speech, it fetches the redner-selected page (`…&top-redner-id={id}`) once to read the precise start and end offsets in seconds (`parse_offset`). Output: `original/media/{session_id}-items.json`.
- `parsers/media2json.py` → flattens to `original/media/{session_id}-media.json` (one record per speech: name split into `Firstname Lastname`, `fraktion`→faction, `funktion`→role, `mdlId`→`originPersonID`, start and end offset, real wall-clock `start`/`end`).
- `scraper/fetch_proceedings.py` → fetches `MMP{wp}-{n}.html` into `original/proceedings/{session_id}.html`. The protocol follows the sitting by a few days; until then the URL answers 404 and the session is tried again on the next run.
- `parsers/proceedings2json.py` → parses the protocol into `original/proceedings/{session_id}-proceedings.json`, one text turn per Mediathek speech (see Merge strategy).

The Mediathek video page is **static, server-rendered HTML**. The per-speech spine lives in `<!-- TEST-REDNER: Redner{mdlId=…, funktionId=…, name=…, fraktion=…, funktion=…, topNr=…} -->` debug comments, each followed by a `?kid=…&top-redner-id={id}` seek link; the TOP title is the nearest preceding `<h3 class="e-top__title">`; the scheduled session start is in `<time datetime="…">`. There is **one HLS stream per session** (`/videos/{kid}/playlist.m3u8`); the base page exposes only minute-resolution display times, while the precise per-speech offsets are rendered (in seconds) by the player only on a redner-selected page.

The speech list covers the plenary sessions from sitting 67 (2024-06-12). Earlier video pages (WP 18 sittings 1–66 and all recordings back to 2014-07-02) carry the recording but no speech list, so discovery skips them. Within a sitting, the list has no entries for the Fragestunde or for items without debate, and none for interposed questions or short interventions; the chair has entries only for the opening of the sitting.

The Plenarprotokoll HTML is a Word export. Each contribution starts with a `rRednerkopf` paragraph holding the speaker line ("Klaus Esser*) (AfD): …", "Präsident André Kuper: …", "Verena Schäffer, Ministerin für …: …"); interposed questions and short interventions use `fZwischenfrage`, interjections `kKlammer`, quotations `zZitat`, and numbered agenda headings `t-N-O…`. The body ends at the first `sSchluss` paragraph; the appendix after it holds speeches given only to the record ("zu Protokoll gegebene Reden"), which have no video and are not parsed.

## Merge strategy

The media stream is the authoritative spine: `merger/merge_session.py` translates the per-speech intermediate records into Stage 2 and joins the protocol text onto them. One `agendaItem` per TOP (`id = "TOP-{n}"` when numbered, else a capped title slug, classified via `optv.shared.agenda_types.classify_de_nw`), one `people[]` entry per speech (carrying `originPersonID` = the native MdL id / funktionId, plus the presiding officers named in the matched text), and media as the session HLS stream with an HTML5 media-fragment `#t=start,end` on `videoFileURI` + `startOffset`/`endOffset` in `additionalInformation` (the SE/DE-SH per-speech offset model). `sourcePage` is the session page with the per-speech `&top-redner-id={id}` query — already unique per speech (the platform keys speech identity on it). `originID` is `{session}-{top-redner-id}`.

Speech `dateStart`/`dateEnd` are **real wall-clock** (session start + offset), so `debug.timesAreVideoRelative = false`. The page's `<time datetime>` carries the scheduled local start with the server's UTC offset on the day of the request, not the offset of the sitting's date; `parsers/media2json.py` keeps its local date and time and localises them to Europe/Berlin.

Each speech's window ends at the next listed speech's start, which keeps the chair's hand-over to the next speaker inside the clip. When the next listed speech starts more than 5 minutes after the speech's own end (an item without a speech list lies in between, e.g. the Fragestunde), the speech's own end is used. The chair's entries (the opening of the sitting) always run to the next listed speech, because the chair conducts everything up to it (e.g. a swearing-in). The last speech keeps its own end.

The protocol's speaker lines are grouped into one text turn per Mediathek speech: a numbered agenda heading closes the current turn; chair lines before the first heading form the opening turn; other chair lines, interposed questions and short interventions join the current turn; a line by the turn's main speaker or one of its questioners continues it (a speech interrupted by the chair, an answer to a question); any other speaker line opens a new turn. The merger groups both sides by agenda item (the spine's `topNr`, the protocol's numbered headings) and matches surnames within each group (`join_text_to_spine`, Needleman-Wunsch), so text never crosses into another agenda item. Each speech body keeps its speaker: the main speaker, the chair (`president` / `vice-president`) or a questioner (`speaker`).

## Running

```sh
# Routine run (newest period): download, merge, link entities
./workflow.py /path/to/OpenParliamentTV-Data-DE-NW \
    --download-original --merge-speeches --link-entities

# Pre-NEL, (re)generate the local entity dump (Wikidata P39 Q17781726 + WP-18 parties):
python -m optv.shared.entity_dump_bootstrap DE-NW /path/to/OpenParliamentTV-Data-DE-NW

# Explicit, single test session (targeted via --kid, no archive pagination):
./workflow.py --period=18 /path/to/OpenParliamentTV-Data-DE-NW \
    --download-original --merge-speeches --link-entities \
    --kid 16904f0f-9e3d-4b7c-a9a2-ad09bd26ac69

# Validate the published output:
python -m optv.shared.validators.cli --dir /path/to/OpenParliamentTV-Data-DE-NW/processed --schema full
```

## Access notes

The Mediathek and the protocols are plain public HTML — no auth, no anti-bot, no headless browser needed (a UA-spoofed `urllib` GET works). Each session is one HLS stream (`/videos/{kid}/playlist.m3u8`) with per-speech seek by `top-redner-id`; the player resolves a `top-redner-id` to precise seconds offsets server-side, so the scraper makes one extra GET per speech. The MdL id (`mdlId`) is exposed in the page's debug comments and carried as `originPersonID`.

## Known limitations

- **Experimental, unvalidated text path.** The protocol text is joined onto the spine and `align`/`ner` run on the result, but no Whisper-QC/text-fidelity audit has cleared it. **Not ready for platform integration.**
- **Media license.** The Landtag publishes no terms of use for the Mediathek. The Impressum's general clause says that video sequences may not be reproduced or used in other publications without the author's consent, so platform use needs the Landtag's consent.
- **Minute display times.** The base page's display times are minute-resolution; the precise offsets need one extra GET per speech (~48–110 per session), with retry/backoff and a polite delay.
- **NEL coverage caveat** (DE-HH/DE-NI/DE-BW class): the entity dump is built from Wikidata `P39 wd:Q17781726` ("member of the Landtag of NRW"); it misses current WP-18 members lacking that statement and government members (Minister/-in) who are not sitting MdL. The native `mdlId` (carried as `originPersonID`) is the obvious future precise bridge.
- **Opening with two chair entries.** When the speech list has two chair entries for the opening ("Sitzungseröffnung", "Fortsetzung der Sitzungseröffnung"), the protocol's opening text goes to one of them.
- **WP 18 from sitting 67 only.** The archive covers plenary sessions back to 2014, but the speech list starts with sitting 67 (2024-06-12).
