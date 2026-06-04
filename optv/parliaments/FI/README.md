# Eduskunta (FI)

Pipeline for the Finnish parliament (Eduskunta). Implements the Stage 1 + merge
stages that turn Eduskunta's open data into Stage 2 JSON. See
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context and the data contract.

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
- **Verbatim text** — `scraper/fetch_proceedings.py` pulls the PTK plenary
  minutes XML from the avoindata `VaskiData` document store (keyed by
  `Eduskuntatunnus = "PTK {number}/{year} vp"`), bypassing the bot-protected
  `eduskunta.fi` web pages. `parsers/proceedings2json.py` parses the Eduskunta
  XML (`PuheenvuoroToimenpide` → speaker `Henkilo/@muuTunnus` = personNumber,
  `KappaleKooste` paragraphs, `kieliKoodi` = per-speech language) into
  Stage-2-shaped proceedings, sentence-segmented with the Finnish spaCy model.
- **Speaker → Wikidata** — `scraper/build_entity_dump.py` joins the avoindata
  `MemberOfParliament` roster to a Wikidata SPARQL query
  (`P39 wd:Q17592486`, "member of the Parliament of Finland") by name, writing
  `metadata/entities.json` for NEL.

On-disk: raw downloads in `original/{media,proceedings}/`
(`{session}-event.json`, `{session}-ptk.xml`), parsed intermediates as
`{session}-{media,proceedings}.json`, cache stages in
`cache/{merged,aligned,audio,audio_session}/`, published Stage 2 in
`processed/`.

## Merge strategy

Media-spine merge (`merger/merge_session.py`, same shape as SE/EU/NO): iterate
the broadcast `speakers[]`, and for each clip graft the matching PTK speech.
The join key is `personNumber` + start time — the PTK
`puheenvuoroAloitusHetki` (naive Europe/Helsinki) is converted to UTC and
matched to the broadcast `timeStamp`, picking the nearest unused PTK speech per
member (robust when a member speaks several times). Video without matching text
is kept media-only with empty `textContents`; PTK speeches without video are
dropped.

## Running

```
./optv/parliaments/FI/workflow.py --period=2023 <data_dir> \
    --session 2026-058 \
    --download-original --merge-speeches --link-entities --align-sentences
```

`--period` is the vaalikausi (electoral term) start year. Session keys are
`{year}-{number:03d}` (e.g. `2026-058`). `--session YYYY-NNN` (repeatable)
selects sessions explicitly; without it, plenary sessions are discovered from
`SaliDBIstunto` for the term years. The `update` wrapper bakes in
`--period=2023` and all stage flags. `make download` / `make merge` are
convenience targets.

## Known limitations

- **No NER.** `supported_stages` omits `ner`: the reference entity-fishing
  instance ships no Finnish KB, and Finnish (Finno-Ugric) has no usable cognate
  fallback (unlike NO→Swedish). NEL still links speakers/factions to Wikidata.
- **Uniform-Finnish pipeline.** Swedish-language speeches (~1–3% of plenary)
  are transcribed in Swedish in the PTK; their `originalLanguage` is recorded
  as `sv`, but spaCy sentence segmentation and aeneas alignment still run in
  Finnish, so those speeches degrade slightly. Per-speech model switching was
  deliberately not built (the EU experiment with it was reverted).
- **Two-level period mismatch.** Sessions are keyed by valtiopäivät year +
  number, which spans the term, while `electoralPeriod.number` is the single
  term start year (`2023`). `session.number` is encoded
  `(year - 2023) * 1000 + number` to stay collision-free across the term's
  years; the raw `{year}/{number}` is in `meta.sourceLabel`.
- **PTK ingest lag.** `SaliDBIstunto` occasionally lags the latest session by a
  day; the PTK XML and broadcast page are already available, so pass
  `--session` to process it before discovery catches up.
