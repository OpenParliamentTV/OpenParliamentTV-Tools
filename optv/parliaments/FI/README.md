# Eduskunta (FI)

Pipeline for the Finnish parliament (Eduskunta). Implements the Stage 1 + merge
stages that turn Eduskunta's open data into Stage 2 JSON. See
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context and the data contract. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Pending: move to the new open data API (before the end of 2026)

The implementation needs to be updated. The text source and session discovery use `avoindata.eduskunta.fi` (`VaskiData`, `SaliDBIstunto`, `MemberOfParliament`), which Eduskunta will shut down at the end of 2026; its address will then point to the new service. Eduskunta has recommended the new open data API since 2026-03-30: https://api.eduskunta.fi/ (documented with OpenAPI, spec at https://api.eduskunta.fi/openapi.json, no key, 180 GET / 90 POST requests per minute per IP, CC BY 4.0). Checked on 2026-10-26:

- Speeches: `POST https://api.eduskunta.fi/api/v1/search` with `{"category": "puheenvuoro", "maxResults": 1000, "expression": {"and": [{"property": "valtiopaivavuosi", "match": "2026"}, {"property": "taysistuntonumero", "match": "102"}]}}` returns one JSON record per speech of sitting 102/2026 (127 records, the same as the broadcast `speakers[]`). Each record has a speech ID (`PUH 102/2026/5/1/6`), the agenda item (`poytakirjanasiankohta.kohtanumero`, `nimeketeksti`), the speaker (`puhuja.henkilonro` = `personNumber`, names, `lisatieto` = party, `eduskuntaryhma_tunnus`), `aloitushetki` / `lopetushetki` in UTC (identical to the broadcast `timeStamp` / `endTimeStamp`), the speech text (`puheenvuoro`) and the chair's following remark. Data is updated in near real time; sitting 104/2026 (2026-10-23) was available on 2026-10-26, while `SaliDBIstunto` still lacked it.
- Full minutes: `GET https://api.eduskunta.fi/api/v1/asiakirjat/edktunnus/{edktunnus}/xml` returns the PTK in the same `Siirto` XML as `VaskiData` (also `/html`, `/pdf`).
- MPs: the spec lists `/api/v1/kansanedustajat/{id}` and `/api/v1/reference-data/kansanedustajat` (not tested yet) as replacements for `MemberOfParliament` in `scraper/build_entity_dump.py`.

Related findings from the same check:

- `dateStart` is the PTK's naive local time (e.g. `2026-05-29T13:07:28`), but Stage 2 requires a time zone. The API's UTC times fix this.
- `kieliKoodi` is `fi` for every speech, including Swedish ones (all 40,647 values in the 408 PTK files of OpenParliamentTV-Data-FI), so `originalLanguage` is never `sv`. Swedish speeches have to be detected from the text. The "Uniform-Finnish pipeline" limitation below is therefore stated too optimistically.
- The broadcast platform serves a per-speech MP4: `https://verkkolahetys.eduskunta.fi/api/events/taysistunto-{number}-{year}/download/video?start={time}&end={endTime}` redirects to an MP4 cut to that range (tested: 325.0 s for 2025–2350 of 102/2026). This could replace the HLS master + `#t=` fragment as `videoFileURI` and simplify `align_prep.py`. The same session data as the RSC flight payload is available as JSON from `https://verkkolahetys.eduskunta.fi/api/events/slug/taysistunto-{number}-{year}/eventMeta` (speakers, topics) and `/api/events/slug/taysistunto-{number}-{year}` (`hlsUrl`). None of these endpoints is documented.
- eduskunta.fi was relaunched: the `eduskunta.fi/FI/vaski/...` pages return 404 and the new pages have no CAPTCHA. The minutes of an agenda item are at `https://www.eduskunta.fi/valtiopaivaasiakirjat/PTK+{number}/{year}+{item}`, with an anchor per speech (e.g. `#14.33Lohikoski`).

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
