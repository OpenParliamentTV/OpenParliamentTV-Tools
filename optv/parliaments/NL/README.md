# Tweede Kamer (NL)

This directory implements the Open Parliament TV pipeline for the plenary sittings of the Dutch House of Representatives (Tweede Kamer der Staten-Generaal). The Senate is not covered. Repo-wide context is in [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md); how this parliament's data shape fits the cross-parliament model is in [DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Two streams per sitting. The report is the spine: every speech, its text and its start and end clock times come from it. Debat Direct, the House's video site, adds the recording.

- **Proceedings (the spine).** [`scraper/fetch.py`](scraper/fetch.py) reads the House's open data API (`gegevensmagazijn.tweedekamer.nl/OData/v5`): the sitting's record (`Vergadering`) with its report versions (`Verslag`), and the XML of one version. The corrected final report is taken when it exists, otherwise the newest interim version. [`parsers/proceedings2json.py`](parsers/proceedings2json.py) turns the XML into speeches.
- **Media.** The same scraper reads Debat Direct's API (`cdn.debatdirect.tweedekamer.nl/api`): the plenary hall's agenda for the sitting's day and the record of each debate the report refers to. A debate has the id of the report's agenda item, names the day's recording of the plenary hall (HLS) and lists the moments Debat Direct marks in it. [`parsers/media2json.py`](parsers/media2json.py) keeps one record per debate.

On disk: `original/proceedings/{session}-vergadering.json` (sitting record and the report version fetched), `original/proceedings/{session}-verslag.xml`, `original/media/{session}-debatdirect.json`; the parsed files are `{session}-proceedings.json` and `{session}-media.json`.

**Session keys and periods** ([`common.py`](common.py)). The House numbers its sittings within a session year (September to September), so a session key is `{year}-{number:03d}`, e.g. `2026-005` for the 5th sitting of 2026-2027. The House does not number its terms; `--period` is the year a House was elected (`2025`). A session year can belong to two terms, so the term of a session is read from `TERM_FIRST_SESSION`, which needs a new entry when a newly elected House first meets. `session.number` is `(year - period) * 1000 + number`.

**What a speech is.** The report records floor turns (`Spreekbeurt`), each with its passages, interruptions and inserted record texts individually timed. One speech is one floor turn, with three adjustments:

- A turn shorter than 15 seconds joins the speech before it, or the one after it when nothing precedes it. A debate request in the order of business thus becomes one speech: the request, the chair's question and every "Steun." with its own speaker. A suspension, a pause of more than 30 seconds and a change of agenda item end the joining.
- A turn longer than 30 minutes is cut into parts of about equal length where its speaker takes the floor again after an interruption (`debug.part`, e.g. `2/3`).
- A run of voting items, which has no speaker in the report, is one speech of the chair.

`debug.turns` says how many turns a speech holds. Text nobody speaks (suspensions, references, the wording of an amended motion, what is put to the vote) is kept as `comment` blocks of one sentence each.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) joins by id and by clock time; nothing is matched by content. A speech's agenda item carries the id of its Debat Direct debate. `media.videoFileURI` is the day's recording with the speech's start and end as `start` and `end` parameters, which the stream server cuts out. `media.sourcePage` is the debate's page on Debat Direct with the moment the speech starts at (`?event=`); where Debat Direct marks no moment in the speech, the link names the speech's start time. A speech of an agenda item Debat Direct does not list (usually the opening) is cut from the same day's recording and links to the House's report page, with the speech's id as fragment to keep the address unique.

The report page of the sitting is `textContents[].sourceURI`. The report's id of the turn is `textContents[].originTextID`, the debate's id `media.originMediaID`; there is no joint id.

## Running

```bash
# Named sittings: download, parse, merge, link speakers, align.
./optv/parliaments/NL/workflow.py --period=2025 <data_dir> \
    --session 2026-005 --session 2026-023 --limit-session '^(2026-005|2026-023)$' \
    --download-original --merge-speeches --link-entities --align-sentences

# Every sitting of the period that is missing or has a newer report version.
./optv/parliaments/NL/workflow.py --period=2025 <data_dir> --download-original --merge-speeches
```

`--session <key>` (repeatable) names the sittings to download; without it the scraper lists the period's sittings, one request per session year. `--limit-session <regex>` confines the download and every later stage. A sitting whose stored report is the final one is skipped without a request.

Build the list of speakers and factions once, and again when the House's membership changes: `python -m optv.parliaments.NL.scraper.build_entity_dump <data_dir>` writes `metadata/entities.json` from Wikidata.

A sitting whose report was replaced by a newer version is queued in `cache/nl-report-upgrades.txt`, and the workflow re-runs it with rebuild semantics on a run that merges.

## Access notes

- The open data API is documented and needs no registration. The Debat Direct API and the stream's `start` and `end` parameters are what the Debat Direct site itself uses; they are not documented.
- The stream's segments are served by a content delivery network on another host. Aligning a sitting fetches the audio rendition of every speech, about 940 segments per hour of speech.
- The House's licence terms for its recordings (`media.license` in the manifest) do not allow keeping obtained material for more than a month. The audio the align stage caches under `cache/audio` falls under that.

## Known limitations

- **Not validated.** No comparison of the published text with what is said in the recording has been made, and alignment quality is unknown. Not ready for platform integration.
- **Scope.** The House's plenary sittings of the period 2025. Sittings that run past midnight are untested: the recording is addressed per calendar day.
- **Clips begin and end at segment boundaries.** The stream server answers a time range with the whole segments that cover it (3.84 seconds each), so a clip can start up to that much before the speech and end as much after it. The alignment audio is the same cut, so sentence times refer to the clip as a player shows it.
- **The chair's formal announcements are printed in full but not read out in full.** In the order of business the report gives lists of documents and appointments as the chair's words. Sentence times in those speeches are not reliable.
- **Two readings of the report are assumptions.** The wording of a motion counts as read out by its mover; the registration line that follows it ("Deze motie is voorgesteld door …") counts as record text.
- **Period and session numbers do not fit the platform's slots.** `electoralPeriod.number` is a year, and `session.number` has four digits from a term's second session year on.
- **Speakers are linked by name.** A member the report names differently from Wikidata stays unlinked until Wikidata lists that name as an alias; a member whose Wikidata item has no membership statement is not in the list at all. One-member factions ("Lid …") have no Wikidata item.
- **No entity extraction.** The entity-fishing service in use does not support Dutch, so `ner` is not among the supported stages.
