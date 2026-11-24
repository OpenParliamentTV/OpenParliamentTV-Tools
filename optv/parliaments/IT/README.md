# Camera dei deputati (IT)

The Italian Chamber of Deputies: the sittings of its assembly, from the Chamber's WebTV and its stenographic record. The Senate is a separate institution with its own video platform and records and is not covered. Repo-wide context is in [docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md); how this parliament's data shape fits the cross-parliament model is in [DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

A session is one sitting (*seduta*). Its key is `{legislature}{sitting:04d}`, e.g. `190731` for sitting 731 of the XIX legislature; `--period` is the legislature.

Two input streams, fetched by [scraper/fetch.py](scraper/fetch.py):

- **Media, the spine.** The sitting's page on the WebTV (`webtv.camera.it/evento/{event}`), stored as `original/media/{session}-evento.html`. It lists every intervention of the sitting with speaker, role and group, its start and end offset in the recording, the MP4 file of each part of the recording, and a tree of titles (agenda item, subject, phase). For most interventions it also names the id of the record intervention they start with. [parsers/media2json.py](parsers/media2json.py) turns it into `original/media/{session}-media.json`. The event id of a sitting comes from the WebTV archive service, whose entry is kept as `original/media/{session}-archivio.json`.
- **Proceedings.** The XML version of the stenographic record (`original/proceedings/{session}-stenografico.xml`), one `intervento` per intervention with the speaker's id and name. [parsers/proceedings2json.py](parsers/proceedings2json.py) turns it into `original/proceedings/{session}-proceedings.json`: text as sentences, what the record sets in italic parentheses (`(Applausi)`, `(Segue la votazione)`) as comments, the annex of texts that were handed in but not spoken left out.

A speech's video is the sitting's own MP4 with a media fragment (`…mp4#t=start,end`); nothing is cut or re-hosted. Its source page is the WebTV address that opens the player at the intervention (`…/evento/{event}/{intervention}`).

The Chamber publishes a recording in one quality only, 1080p, as a file of 4 to 10 GB per sitting and as a stream of the same data; there is no audio rendition. For the align stage, [align_prep.py](align_prep.py) downloads each MP4 once as a file, writes its audio track into the cache, deletes the MP4 and slices the audio per speech. The file is asked for with one request; where the server ends the transfer before the file is whole, a further request continues it from that point.

Speakers carry the Chamber's person id (`people[].originPersonID`). [scraper/build_entity_dump.py](scraper/build_entity_dump.py) builds `metadata/entities.json` from Wikidata, where deputies have that id (`P1341`), so they are linked by id; senators and ministers are linked by name.

## Merge strategy

[merger/merge_session.py](merger/merge_session.py) makes one speech of every intervention the WebTV page lists and joins the record text by id. The ids the page names are the anchors. The record has more interventions than the page, because it sets apart what the chair says in between and what a speaker says after an interruption; these lie between two anchors in the record and in the recording alike, and are distributed over the page's interventions there in order. An intervention the page does not anchor (a vote, the opening) opens with the first fitting record intervention of its own speaker, and of the possible distributions the one that puts the least text on a speech of another speaker is taken. Nothing is matched by content.

The agenda item is the subject in the page's title tree (the bill, the single interpellation), or the item itself where it has no subject; the phase (`Dichiarazioni di voto finale`, `Risposta`) is in `debug.phase`. A phase in which the House votes has the type `voting`.

## Running

```
python -m optv.parliaments.IT.workflow <data_dir> --period=19 \
    --session 190731 --limit-session '^190731$' \
    --download-original --merge-speeches --link-entities --align-sentences
```

- `--session` (repeatable) names the sittings to download. Each one is looked up in the archive service and then costs two requests, its page and its record.
- Without `--session` the download lists the legislature in the archive, 20 sittings per request, newest first. It stops at the first page that holds nothing new; with `--limit-session` or `--force` it lists the whole legislature, about 40 requests for 750 sittings, before it fetches what is in scope.
- A sitting whose page links the record and whose record is stored is not requested again. One that was fetched before its record was online is fetched again on the next run that covers it.
- `--align-sentences` transfers the whole recording of every sitting it aligns, 4 to 10 GB each, and needs that much free disk space while a download runs. A sitting whose audio is in the cache is not downloaded again. A transfer the server ends early is continued up to five times within a run; what is still missing then stays as a partial file that the next run continues.
- `python -m optv.parliaments.IT.scraper.build_entity_dump <data_dir>` builds the entity list for `--link-entities`; it sends three queries to Wikidata, the largest of which can take several attempts.

## Access notes

The WebTV archive service, the sitting page and the record XML are not documented by the Chamber; their addresses and fields are read from the WebTV's own pages. The archive service is a form POST that answers XML.

## Known limitations

- Not ready for platform integration. In the Whisper comparison of the sample sittings, more speeches have a text that differs from what is heard than the readiness criteria allow, and no person has reviewed the comparison. The figures are in the [scorecard](../../../docs/readiness/IT.json).
- The Chamber's legal notice licenses texts and recordings as CC BY-NC-ND 4.0 and asks for its written permission for derivative works. Whether publishing them here is covered is an open question for a person to settle.
- Aligning a whole legislature means transferring every recording in full: about 750 sittings of 4 to 10 GB each for the XIX legislature. That is not something to start without the Chamber's agreement or another source of audio.
- No entity extraction (`ner`): the entity-fishing service in use has no Italian knowledge base loaded.
- The reading of the minutes is a clip whose own words the record does not give; it only mentions that the minutes were read. The speech keeps the chair's closing sentence and is marked `debug.confidence` 0.5 (`confidenceReason: no-own-text`), so that its text is not imported.
- What the chair says between two speakers is in the clip of the speaker before, as the page's time ranges have it. The text is then in the right clip, under a speech of another main speaker.
- A vote is an intervention of the chair on the page, and its time range begins where the vote is called or already open. The record intervention it carries begins earlier, with what the chair says before calling the vote. Those sentences stand in the vote's speech and are spoken in the clip before.
- The record is an edited text. Where the chair reads out the title of an interpellation, the record refers to the annex; an announcement is completed with details that are not read out. The text of such a speech is shorter or longer than what is heard.
- A speech whose text does not fit its time range is marked `debug.confidence` 0.5 (`text-too-long`, `text-too-short`). The limits are set from three sittings.
- The last speech of a sitting has no end on the page; it runs to the end the archive states for the sitting. That end can lie up to half a minute after the end of the recording; the speech's audio then ends where the recording does.
- The record prints the agenda of the next sitting in the chair's closing intervention, with bill numbers, joined bills and rapporteurs, and the chair reads a short form of it. All of it is part of the last speech's text, so with an agenda of several items that speech has several times the text that is heard. The record gives the agenda's paragraphs ids of their own (`iv.N.odg.…`); the parser reads them like the chair's other paragraphs.
- A suspension of the sitting is listed by the page as an intervention of the chair. It is left out when it carries no text.
- Recordings in several parts are handled as the page describes them (each intervention names its part); the sittings processed so far have one part each.
- Sittings before the XIX legislature have not been looked at; the page and the record may be structured differently there.
- A speaker of the Mixed Group has the group `MISTO` from the page. Where only the record names the group, it is the abbreviation with the component (`MISTO-…`), which the entity list does not know.
- Documents under discussion (the record's `atto` references) are not carried into `documents`.
