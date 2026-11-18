# Landtag von Sachsen-Anhalt (DE-ST)

Parser/merger for the Saxony-Anhalt state parliament (Wahlperiode 9, since October 2026). Proceedings and media come from the same Landtag portal (`landtag.sachsen-anhalt.de`); the per-speech text-video join is in the DOM, so no Needleman-Wunsch alignment is needed. For repo-wide context and onboarding, see [`docs/ADDING-A-PARLIAMENT.md`](../../../docs/ADDING-A-PARLIAMENT.md). For how its data shape compares to the cross-parliament model, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

The portal keeps only the electoral period in progress online: the pages and videos of a period go offline when it ends (the Landtag archive keeps them). Each sitting has to be imported while its period runs.

## Data model

A single source page per **Sitzungsperiode** bundles 1–3 **Landtagssitzungen** (calendar days). We treat each Sitzung as the OPTV `session` (matching DE/DE-RP's convention). One scraped page therefore yields N intermediate proceedings files and N media file collections — one per day section (`<section id="section-N">`).

- **Archive crawler** ([`scraper/fetch_archive.py`](scraper/fetch_archive.py)): reads the Sitzungsperioden listed under `<h2>{N}. Wahlperiode</h2>` on `/archiv`. Page addresses are editor-chosen (`/1-sitzungsperiode`, `/2-sitzungsperiode-1`), so each page's href is used as it is and the SP number comes from the link text. Pages are cached as `original/proceedings/sp-{PP}-{NNN}.html`; a cached page is fetched again while one of its days still lacks a protocol link. The map of sittings goes to `<data>/metadata/sitzung-map-{PP}.json` (see "Sitzung numbering"). A run for a period the archive no longer lists downloads nothing.
- **Per-speech video** ([`scraper/fetch_media.py`](scraper/fetch_media.py)): for every standard video player-id (`data-js-id="video-std"`; sign-language `video-sign` skipped), GETs `{SP page}?videoSessions=videoAjax&videoId={pid}` and saves the raw HTML under `original/media/{PPNNN}/{player-id}.html`. The AJAX response carries a `data-jsb` JSON blob with the MP4 URLs (1080p/720p/360p), duration, and thumbnail.
- **Protocol** ([`scraper/fetch_protocols.py`](scraper/fetch_protocols.py)): each day section links that sitting's Plenarprotokoll (`/fileadmin/files/plenum/wp{P}/{NNN}stzg.pdf`) once it is published; the PDF is saved as `original/proceedings/{PPNNN}.pdf`. The link redirects to the PADOKA documentation server.
- **Per-Sitzung proceedings** ([`parsers/proceedings2json.py`](parsers/proceedings2json.py)): splits the SP page by day section and emits `{PPNNN}-proceedings.json` for each Sitzung. Each speech record carries speaker label, party, role, TOP number + title, std/sign player-ids, the clip's page anchor (`#section-video-{n}-{m}`) and the transcript URLs as the page links them. With the protocol on disk, `meta.sittingTimes` holds the sitting's start, breaks and end. A file is rewritten only when its content changes.
- **Per-Sitzung media** ([`parsers/media2json.py`](parsers/media2json.py)): decodes the `data-jsb` JSON on each AJAX response and aggregates `{PPNNN}-media.json` keyed by player-id (duration, thumbnail, MP4 URLs at three qualities).
- **Transcript fetch** is deferred to the merger to keep the proceedings parser cheap and pure; cached under `original/proceedings/transcripts/{PPNNN}/{speaker-id}.html`. The AJAX transcript URL is used exactly as linked: its `cHash` is bound to the SP page's path.

## Sitzung numbering

The portal groups plenary work by **Sitzungsperiode** and shows the canonical **Landtagssitzung** number only in text (transcript prose, agenda and protocol PDFs). The number of each sitting day is:

1. the number in the file name of the protocol the day section links (`{NNN}stzg.pdf`), once the protocol is published;
2. otherwise the previous day's number + 1, in archive order (days before the first protocol count back from it; without any protocol, numbering starts at 1).

`numberSource` in the map records which rule applied. A protocol number that disagrees with the sequence is logged.

## Merge strategy

[`merger/merge_session.py`](merger/merge_session.py) does a **1:1 ordered zip** on the standard-video player-id (no Needleman-Wunsch). Every video-list-item on the SP page produces one entry on both streams; their order in DOM is the join order.

The transcript of a clip is the text of the Stenografischer Bericht for that clip, with interjections. It holds one or more contributions, each opened by a speaker line (`Präsident Tobias Rausch:`, `Daniel Roi (AfD):`, `Matthias Büttner (Staßfurt) (AfD):`, `Sven Schulze (Ministerpräsident):`). The merger:

- drops the agenda headings between speeches (`Tagesordnungspunkt 30`, the item's title and printed-paper lines, `Erste Beratung`, `Abstimmung`);
- makes each contribution its own `textBody` item with `speaker`/`speakerstatus`, and `(…)` paragraphs `comment` items;
- attributes text before the first speaker line to the last speaker of the previous clip of the sitting (the protocol writes no new speaker line when the speaker continues);
- puts everyone who speaks into `people[]`: the slot's speaker first (`main-speaker`), then presidium (`president`/`vice-president`/`interim-president`) and other speakers (`speaker`). A procedural slot (`Eröffnung`, `Abstimmung`, `Wahl`, `Schlussbemerkungen`) has no person in its heading, so its first contribution's speaker — the presiding officer — is the main speaker. A speaker whose line names only an office gets the faction their slot heading shows elsewhere in the sitting.

`media.sourcePage` is the SP page with the clip's anchor (`#section-video-{n}-{m}`), which opens that clip; the anchor is unique on the page. `textContents[].sourceURI` is the clip's stand-alone transcript page.

**Times.** `dateStart`/`dateEnd` are German wall-clock with the Europe/Berlin UTC offset. The clock starts at the protocol's `Beginn` and runs on by each clip's duration; at each `Unterbrechung`/`Wiederbeginn` pair it moves to the resumption time at the clip whose transcript contains the protocol text the sitting resumed with (a break that matches no clip is placed by clock time). The page lists the clips by agenda item, but a sitting can take items out of order (e.g. part of TOP 6 after TOP 8); with the protocol, the clock and the carried-over speaker follow the protocol's order instead, found by locating each clip's first speech paragraph in the protocol text (`debug.sittingOrder`; `speechIndex` keeps the page order). Until the protocol is published, the clock starts at 09:00 and follows the page order (`debug.sittingTimesSource = "default"`, else `"protocol"`); the next run after its publication re-merges the sitting. Gaps between clips are not in the durations, so times drift a little within each part of a sitting. `session.dateStart`/`dateEnd` are the protocol's start and end (without protocol: first and last clip).

Agenda-item classification (TOP titles → `nativeType` / `type`) uses [`classify_de_st`](../../shared/agenda_types.py) — `Wahl` → election, `Vereidigung` → oath, `Eröffnung` → opening, `Erste/Zweite/Dritte Beratung` → regular, `Aktuelle Debatte` → current_affairs, `Befragung der Landesregierung` → questioning_of_the_government.

## Entity dump

`python -m optv.shared.entity_dump_bootstrap DE-ST <data_dir>` queries Wikidata for everyone with `P39 wd:Q18559580` ("member of the Landtag of Saxony-Anhalt") and combines them with the party list of WP 8 and WP 9 (incl. BSW) in [`manifest.yaml`](manifest.yaml) (`entity_bootstrap`). Writes `<data>/metadata/entities.json` in the format `optv.shared.nel.get_nel_data` expects. The `entity_dump_url` in [`manifest.yaml`](manifest.yaml) is empty; the workflow uses the local file.

## Running

```bash
./optv/parliaments/DE-ST/workflow.py --period=9 <data_dir> \
    --limit-session '0900[1-3]' \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

The session regex scopes the media/protocol download and the downstream stages to the sample sittings 1–3; change or drop `--limit-session` to widen.

## Access notes

The portal is plain nginx + TYPO3 with no anti-bot — stdlib `urllib` with a polite User-Agent suffices. The video AJAX endpoint and the transcript AJAX endpoint are undocumented (not a stable API). **nginx rate-limits at ~60 requests / 60 s window per IP**; the scraper enforces a 1.5 s global throttle (`POLITE_DELAY` in [`scraper/common.py`](scraper/common.py)) to stay under it. A 404/410 is not retried. The protocol PDFs are served by PADOKA (`padoka.landtag.sachsen-anhalt.de`), whose robots.txt disallows all paths; the adapter fetches one PDF per sitting through the link on the SP page.

## Known limitations

- **Only the current electoral period is online.** The WP 8 sample sittings (08105–08107) in the data repository can't be merged again: their MP4 links no longer resolve, and their `textContents[].license` is still `© Landtag von Sachsen-Anhalt — Schriftdolmetschung`.
- **`sourcePage` is unique within a period only.** Page slugs are reused across periods (`/1-sitzungsperiode` served SP 1 of both WP 8 and WP 9), and the platform's import keys speech identity on the exact `sourcePage`.
- **`entity_dump_url` is empty.** Point the NEL stage at `<data>/metadata/entities.json` produced by `optv.shared.entity_dump_bootstrap`. Non-MP speakers (Landesminister, guests) and members without a Wikidata Landtag statement don't link.
- **Sitzungsperiode dropped from Stage 2.** The portal's URL-grouping unit is recorded only in `meta.sitzungsperiode`. DE-ST is another instance of the three-level temporal pattern (alongside NO/SE) that the current schema flattens.
- **No live API contract.** Video AJAX (`?videoSessions=videoAjax`) and transcript AJAX (`?transcriptSessions=lsaSessionsAjax`) are undocumented TYPO3 extension endpoints. If the portal is rebuilt, both scrapers will need rework.
- **VMS MP4 URLs may rotate.** Hash-named MP4 URLs on `app.vms.landtag-lsa.de` are persisted at scrape time; if the Landtag rotates them, alignment will need to refetch via the AJAX endpoint.
