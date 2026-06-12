# Landtag Nordrhein-Westfalen (DE-NW)

This directory implements the OpenParliamentTV pipeline for the Landtag
Nordrhein-Westfalen (WP 18). It is built on a per-speech video spine; the
Plenarprotokoll PDF is now joined onto that spine via an **experimental,
unvalidated** PDF→TEI text path (see below) — see
[docs/ADDING-A-PARLIAMENT.md](../../../docs/ADDING-A-PARLIAMENT.md) for repo-wide
context and the DE-HH / DE-NI / DE-BW READMEs for the same regime. It combines
the richest signals of that family: a parliament-native MdL id per speech, real
per-speech wall-clock, and precise (second-resolution) per-speech offsets. For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

## Data model

One input stream: **media** (the `www.landtag.nrw.de` Mediathek session pages).
The media stream is the spine. The Plenarprotokoll PDF (`MMP18-{N}.pdf`) is
parsed via `optv.shared.pdf2tei` and joined onto that spine in the merger
(`join_text_to_spine`), so matched speeches carry verbatim `textContents`
(unmatched keep `[]`) and `supported_stages` now includes `align`/`ner` (see Known
limitations).

- `scraper/fetch_archive.py` → the candidate session index
  (`metadata/archive-wp{N}.json`). Sessions are addressed by an opaque `kid`
  (session UUID), so discovery paginates the archive
  (`…/archivierte-aufzeichnungen.html?art=plenarsitzung&page=N`) and scopes cards
  to the WP by date. A `--kid` / `--session` seed records a session directly,
  skipping pagination.
- `scraper/fetch_media.py` → fetches each session video page
  (`/home/mediathek/video.html?kid={kid}`), parses its static `TEST-REDNER`
  markup (`scraper/common.py:parse_video_page`), and derives the authoritative
  Sitzung number from the page's own `<h2>{N}. Plenarsitzung</h2>` header. Then,
  per speech, it fetches the redner-selected page (`…&top-redner-id={id}`) once
  to read the precise start offset in seconds (`parse_offset`). Output:
  `original/media/{session_id}-items.json`.
- `parsers/media2json.py` → flattens to `original/media/{session_id}-media.json`
  (one record per speech: name split into `Firstname Lastname`, `fraktion`→
  faction, `funktion`→role, `mdlId`→`originPersonID`, start offset, synthesised
  end, real wall-clock `start`/`end`).

The Mediathek video page is **static, server-rendered HTML**. The per-speech
spine lives in `<!-- TEST-REDNER: Redner{mdlId=…, funktionId=…, name=…,
fraktion=…, funktion=…, topNr=…} -->` debug comments, each followed by a
`?kid=…&top-redner-id={id}` seek link; the TOP title is the nearest preceding
`<h3 class="e-top__title">`; the session start is in `<time datetime="…+02:00">`.
There is **one HLS stream per session** (`/videos/{kid}/playlist.m3u8`); the base
page exposes only minute-resolution display times, while the precise per-speech
offset is rendered (in seconds) by the player only on a redner-selected page.

## Merge strategy

No cross-source matching: `merger/merge_session.py` is a single-source
translation pass from the per-speech intermediate records into Stage 2 (the media
stream is the authoritative spine). One `agendaItem` per TOP (`id = "TOP-{n}"`
when numbered, else a capped title slug, classified via
`optv.shared.agenda_types.classify_de_nw`), one `people[]` entry per speech
(carrying `originPersonID` = the native MdL id / funktionId), and media as the
session HLS stream with an HTML5 media-fragment `#t=start,end` on `videoFileURI`
+ `startOffset`/`endOffset` in `additionalInformation` (the SE/DE-SH per-speech
offset model). `sourcePage` is the session page with the per-speech
`&top-redner-id={id}` query — already unique per speech (the platform keys speech
identity on it). `originID` is `{session}-{top-redner-id}`. Speech
`dateStart`/`dateEnd` are **real wall-clock** (session start + offset), so
`debug.timesAreVideoRelative = false`. Each window's `endOffset` is synthesised
from the next speech's start (the source's rendered `end` is unreliable for
speeches that double as a TOP "full length" link — the DE-BW approach).

## Running

```sh
# Convenience wrapper (period 18 baked in)
./update /path/to/OpenParliamentTV-Data-DE-NW

# Pre-NEL, (re)generate the local entity dump (Wikidata P39 Q17781726 + WP-18 parties):
python -m optv.parliaments.DE-NW.scraper.build_entity_dump /path/to/OpenParliamentTV-Data-DE-NW

# Explicit, single test session (targeted via --kid, no archive pagination):
./workflow.py --period=18 /path/to/OpenParliamentTV-Data-DE-NW \
    --download-original --merge-speeches --link-entities \
    --kid 16904f0f-9e3d-4b7c-a9a2-ad09bd26ac69

# Validate the published output:
python -m optv.shared.validators.cli --dir /path/to/OpenParliamentTV-Data-DE-NW/processed --schema full
```

Session discovery paginates the archive listing (`?page=1..max`, max read from
page 1) and scopes cards to WP 18 by date; a `--kid`/`--session` seed (or
`metadata/seed-urls.txt`) targets specific sessions without paging.

## Access notes

The Mediathek is plain public HTML — no auth, no anti-bot, no headless browser
needed (a UA-spoofed `urllib` GET works). Each session is one HLS stream
(`/videos/{kid}/playlist.m3u8`) with per-speech seek by `top-redner-id`; the
player resolves a `top-redner-id` to a precise seconds offset server-side, so the
scraper makes one extra GET per speech for the precise start. The MdL id (`mdlId`)
is exposed in the page's debug comments and carried as `originPersonID`.

## Known limitations

- **Experimental, unvalidated text path.** The Plenarprotokoll PDF
  (`MMP18-{N}.pdf`) is parsed via `optv.shared.pdf2tei` and joined onto the spine,
  and `align`/`ner` run on the result. None of this has been validated — there is
  no Whisper-QC/text-fidelity audit yet, the PDF→TEI extraction and the
  text↔spine join still need refinement, and known media-spine timing quirks (see
  below) affect alignment. **Not ready for platform integration.**
- **Coarse per-speech end + minute display times.** Only the *start* offset is
  exposed precisely (per redner page); `endOffset` is synthesised from the next
  speech's start. The base page's display times are minute-resolution.
- **Per-speech offset fetch cost.** One extra GET per speech (~48 per session)
  to read the precise start. Fine for a single session; bulk runs pay it per
  speech (retry/backoff + polite delay apply).
- **NEL coverage caveat** (DE-HH/DE-NI/DE-BW class): the entity dump is built
  from Wikidata `P39 wd:Q17781726` ("member of the Landtag of NRW"); it misses
  current WP-18 members lacking that statement and government members
  (Minister/-in) who are not sitting MdL. The native `mdlId` (carried as
  `originPersonID`) is the obvious future precise bridge.
- **Timezone quirk.** The source stamps the session start `+02:00` even in
  winter; it is emitted as-is (the schema accepts any valid offset).
- **WP 18 only** (the current term). The archive covers plenary sessions back to
  2014, but only WP 18 has been exercised.
