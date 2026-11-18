# Landtag von Baden-Württemberg (DE-BW)

This directory implements the OpenParliamentTV pipeline for the Landtag von
Baden-Württemberg (WP 18 routinely; WP 17 sample sittings). It is built on a per-speech video spine; the
Plenarprotokoll PDF is now joined onto that spine via an **experimental,
unvalidated** PDF→TEI text path (see below) — see
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context and the DE-SH / DE-BY READMEs for the same regime. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

The media stream is the spine (the mediathek video pages). The Plenarprotokoll
PDF is parsed via `optv.shared.pdf2tei` and joined onto that spine in the merger
(`join_text_to_spine`), so matched speeches carry verbatim `textContents`
(unmatched keep `[]`) and `supported_stages` now includes `align`/`ner`. NB BW's
single-debate sessions collapse consecutive same-speaker turns in `parlamint2json`,
which the per-turn video spine does not — the text↔spine join is the main thing
still to refine (see Known limitations).

- `scraper/fetch_archive.py` → the candidate session index (`metadata/archive-wp{N}.json`): walks the mediathek filterlist widget newest first via `?offset=` pagination and scopes to the period by slug date (operator-supplied URLs can be merged in as an override).
- `scraper/fetch_media.py` → fetches each session video page and parses its static `e-chapterList` (`scraper/common.py:parse_video_page`) into `original/media/{session_id}-tops.json` (MP4 URL + per-TOP speech list). A session is refetched when the archive lists a part page its manifest doesn't hold yet.
- `parsers/media2json.py` → flattens to `original/media/{session_id}-media.json` (one record per speech: reordered name, role, faction, start/end offsets). The chapter list names speakers surname first — `Lastname, Firstname` from WP 18 (split at the comma), `Lastname Firstname` before (first token = surname, so multi-word surnames of WP 15–17 come out wrong) — and the parser reorders them to `Firstname Lastname`; trailing particles of the given names move to the surname (`Loga, Cornelia von` → `Cornelia` / `von Loga`); an office written into the name span (`Böhlen, Beate | Bürgerbeauftragte`, empty meta span) becomes the role.
- `parsers/proceedings2json.py` → parses the Plenarprotokoll PDF (`optv.shared.pdf2tei`) into `original/proceedings/{session_id}-proceedings.json`, with the sitting's start, breaks and end from the protocol in `meta.sittingTimes`.

The media stream is the authoritative spine. A Sitzung's recording lives at
`ltbw-stream.babiel.com/wahlperiode{wp}/{year}/sitzung{nr}_{yyyymmdd}/Aufzeichnung_{nr}_{part}.mp4`;
per-speech windows are addressed by start offset (the SE/DE-SH model), not
per-speech files. A long calendar-day Sitzung is split into several sequential
**parts** (`_1.mp4`, `_2.mp4`, …), each its own mediathek page; `fetch_media`
groups the part-cards by (Sitzung, date) and the parser combines them into one
session, each speech keeping its own part's MP4 + per-part offset (TOP numbering
is continuous, so a debate split across the break — `TOP 4` / `Fortsetzung TOP
4` — merges into one `agendaItem`).

## Merge strategy

No cross-source matching: `merger/merge_session.py` is a translation pass from
the per-speech intermediate records into Stage 2. One `agendaItem` per TOP
(`id = "TOP-{n}"`, classified via `optv.shared.agenda_types.classify_de_bw`),
one `people[]` entry per speech, and media as one session MP4 with an HTML5
media-fragment `#t=start,end` on `videoFileURI` + `startOffset`/`endOffset` in
`additionalInformation`. `sourcePage` is the part's page with `?t={start}` — the query the page's player reads to start at the speech — which also makes it unique per speech, as the platform keys speech identity on it. The per-speech `end` is the next speech's start offset (the source carries no per-speech end).

Speech times come from the protocol: it states the sitting's start (`Beginn: 9:33 Uhr`), its breaks (`Unterbrechung der Sitzung` / `Wiederaufnahme der Sitzung`, the lunch break also as `Mittagspause: 12:04 bis 13:16 Uhr` on the cover) and its end (`Schluss`) to the minute. Part 1 of the recording starts at the sitting's start; each further part starts at the resumption after the first break that begins once the previous part's last chapter entry has started, so a short break inside a part (e.g. counting a roll-call vote, recording running on) is skipped. `dateStart` = part start + offset in Europe/Berlin time, `dateEnd` = the next entry's start or, for a part's last entry, the break or `Schluss`; `session.dateStart`/`dateEnd` are the sitting's start and end, and `debug.timesAreVideoRelative = false`, `debug.timeSource = "protocol"`, `debug.partStart` record it. Accurate to about a minute.

## Running

```sh
# Routine run (newest period): download, merge, link entities
./workflow.py /path/to/OpenParliamentTV-Data-DE-BW \
    --download-original --merge-speeches --link-entities

# Explicit, single test session (back-fill via --session):
./workflow.py --period=17 /path/to/OpenParliamentTV-Data-DE-BW \
    --download-original --merge-speeches --link-entities \
    --limit-session '17118' \
    --session 'https://www.landtag-bw.de/de/mediathek/videos/118-sitzung-vom-13-maerz-2025-563198'

# Pre-NEL, regenerate the local entity dump:
python -m optv.parliaments.DE-BW.scraper.build_entity_dump /path/to/OpenParliamentTV-Data-DE-BW
```

Session discovery: `fetch_archive` pages the archive newest first (`?offset=N&noStaticItems=true`) and date-scopes to the period; the result is cached in `archive-wp{N}.json`. A fresh build stops at the first page that lies wholly before the period's start (`WP_DATE_RANGE`); every later run refreshes the cache from the newest pages until a page holds only known sittings, so new sittings are found in a request or two. `--force` rebuilds from scratch; `--max-results N` caps the walk at the newest N items. `--session` / `metadata/seed-urls.txt` remain as a manual override for anything pagination misses.

## Access notes

The mediathek is plain public TYPO3 HTML — no auth, no anti-bot. The per-speech
chapter list is in the static page markup (no JSF state like DE-BY, no
result-cap AJAX like DE-SH). The session number, date and Wahlperiode are read
authoritatively from the babiel MP4 URL embedded in each page. Session discovery
pages the `filterList` widget via `?offset=N&noStaticItems=true` (the param its
"load more" uses); the per-session numeric content-ID in the slug is **not**
guessable (it 404s on its own), so pagination is the only enumeration path.

## Known limitations

- **Experimental, unvalidated text path.** The Plenarprotokoll PDF is parsed via
  `optv.shared.pdf2tei` and joined onto the spine, and `align`/`ner` run on the
  result. None of this has been validated — there is no Whisper-QC/text-fidelity
  audit yet, and the text↔spine join is BW's hardest case (the `parlamint2json`
  continuation-merge folds same-speaker turns against BW's fine per-turn spine).
  **Not ready for platform integration.**
- **Video-relative times until the protocol is out.** The protocol is published after the video. Until it is parsed — or when its times don't fit the video parts (a logged warning) — speech `dateStart`/`dateEnd` are synthetic offsets from midnight of the sitting day without a timezone (`debug.timesAreVideoRelative = true`, `debug.timeSource = "video"`); the real signal is then `media.additionalInformation.startOffset`. The re-merge after the protocol arrives replaces them.
- **NEL coverage caveat** (DE-SH/DE-BY class): Wikidata `P39 wd:Q17481175` misses current members lacking that statement; person-NEL is partial. Fixable downstream by enriching the SPARQL or scraping the member roster (abgeordnetenwatch parliament id 6, CC0).
- **WP 17 and WP 18 only.** Earlier terms are out of scope.
