# Eduskunta (FI)

Pipeline for the Finnish parliament (Eduskunta). Implements the Stage 1 + merge
stages that turn Eduskunta's open data into Stage 2 JSON. See
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context and the data contract. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

Three source streams, joined per session on `personNumber` + speech start time:

- **Video + per-speech timing** — `scraper/fetch_media.py` fetches the
  verkkolähetys broadcast page
  (`verkkolahetys.eduskunta.fi/fi/taysistunnot/taysistunto-{number}-{year}`) and
  extracts the RSC (React Server Component) *flight* payload embedded in the
  page: a `speakers[]` array with `time`/`endTime` (seconds into the session
  video), `personNumber`, `party`, `topicId` and `onkoVastauspuheenvuoro`
  (reply flag), plus the HLS master playlist URL. `parsers/media2json.py` turns
  each speaker into a per-speech media record. **This is the merge spine.**
- **Verbatim text** — `scraper/fetch_proceedings.py` pulls the speech records
  of the sitting from the Eduskunta open data API
  (`POST https://api.eduskunta.fi/api/v1/search`, category `puheenvuoro`;
  documented at https://api.eduskunta.fi/, no key, CC BY 4.0). Each record has
  the speech ID (`PUH 58/2026/6/1/1`), the agenda item, the speaker
  (`henkilonro` = `personNumber`, names, office for ministers, party, group),
  UTC start/end and the text as one string (the chair's interjections are not
  included). `parsers/proceedings2json.py` turns them into Stage-2-shaped
  proceedings, sentence-segmented with the Finnish spaCy model. Swedish
  speeches are detected from the text (function-word ratio), because neither
  the records nor the minutes mark the language.
- **Speaker → Wikidata** — `scraper/build_entity_dump.py` joins the API's MP
  register (`/api/v1/reference-data/kansanedustajat`) to a Wikidata SPARQL
  query (`P39 wd:Q17592486`, "member of the Parliament of Finland") by name,
  writing `metadata/entities.json` for NEL.

The text is revised after the sitting: each record has a status (`tila`)
that starts as `Käsitelty` and becomes `Tarkistettu` or `Hyväksytty` some
weeks later. The download stage fetches a sitting again while it still holds
non-final records and is at most 90 days old, and rewrites the raw file only
when the records changed, so parse/merge/align re-run only on a real revision.

On-disk: raw downloads in `original/{media,proceedings}/`
(`{session}-event.json`, `{session}-speeches.json`), parsed intermediates as
`{session}-{media,proceedings}.json`, cache stages in
`cache/{merged,aligned,audio,audio_session}/`, published Stage 2 in
`processed/`.

## Merge strategy

Media-spine merge (`merger/merge_session.py`, same shape as SE/EU/NO): iterate
the broadcast `speakers[]`, and for each clip graft the matching speech record.
The join key is `personNumber` + start time — the record's start
(`dateStart`, Europe/Helsinki with offset) and the broadcast `timeStamp` (UTC)
are the same instant to the second; the merger picks the nearest unused speech
record per member (robust when a member speaks several times). Video without
matching text is kept media-only with empty `textContents`; speech records
without video are dropped.

## Running

```
./optv/parliaments/FI/workflow.py --period=2023 <data_dir> \
    --session 2026-058 \
    --download-original --merge-speeches --link-entities --align-sentences
```

`--period` is the vaalikausi (electoral term) start year. Session keys are
`{year}-{number:03d}` (e.g. `2026-058`). `--session YYYY-NNN` (repeatable)
selects sessions explicitly; without it, the plenary sittings of the term
years are discovered from the open data API (one aggregation request listing
every sitting that has speech records).

## Known limitations

- **No NER.** `supported_stages` omits `ner`: the reference entity-fishing
  instance ships no Finnish KB, and Finnish (Finno-Ugric) has no usable cognate
  fallback (unlike NO→Swedish). NEL still links speakers/factions to Wikidata.
- **Uniform-Finnish pipeline.** Swedish-language speeches (~3% of plenary)
  are transcribed in Swedish; their `originalLanguage` is detected from the
  text and recorded as `sv`, but spaCy sentence segmentation and aeneas
  alignment still run in Finnish, so those speeches degrade slightly. A speech
  that mixes both languages counts as the language with more function words.
  Per-speech model switching was deliberately not built (the EU experiment with
  it was reverted).
- **Two-level period mismatch.** Sessions are keyed by valtiopäivät year +
  number, which spans the term, while `electoralPeriod.number` is the single
  term start year (`2023`). `session.number` is encoded
  `(year - 2023) * 1000 + number` to stay collision-free across the term's
  years; the raw `{year}/{number}` is in `meta.sourceLabel`.
