# Adding a parliament

This is the end-to-end checklist for onboarding a new parliament into Open Parliament TV. The German Bundestag implementation in [optv/parliaments/DE/](../optv/parliaments/DE/) is the reference; new parliaments follow the same shape.

For the data contract and pipeline concepts, see [Architecture/STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md) and [Architecture/PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md). For how parliaments' data shapes differ structurally, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

---

## 1. Choose a shortcode

Follow [Architecture/SHORTCODES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/SHORTCODES.md). National parliaments use the country's ISO 3166 alpha-2 code (`DE`, `SE`, `FR`); regional parliaments use the ISO subdivision code (`DE-BE`, `DE-BY`); custom codes are allowed where ISO doesn't fit (`US-NYC`, `CAT`). Always uppercase.

The shortcode becomes the directory name (`optv/parliaments/<CODE>/`) and the data-repo suffix (`OpenParliamentTV-Data-<CODE>`).

## 2. Survey the source data

Which parliament to add is a question of how good its sources are. Where the project's survey of parliaments is on the machine, `python -m optv.scripts.readiness next` names the most feasible one that has no implementation yet.

Before writing any code, confirm the parliament publishes the four things the pipeline needs:

- **Video** for every speech, fetchable by URL. Either per-speech files or a session video plus per-speech timecodes. Without accessible video, the parliament is not a viable target.
- **Proceedings / transcripts** in some structured format (TEI XML, ParlaMint, Akoma Ntoso, custom JSON, etc.).
- **A media metadata feed** (RSS, JSON API, …) listing video items with at least the parliament's internal media ID and a public source page.
- **Wikidata coverage** for at least the major speakers. Not strictly required (the NEL stage tolerates gaps), but coverage gaps degrade platform features.

Validation target for the eventual output: [optv/shared/schema/stage2-full.schema.json](../optv/shared/schema/stage2-full.schema.json). Read [optv/shared/schema/README.md](../optv/shared/schema/README.md) before designing the parser — it documents enum values, datetime patterns, and conventions that the parser must satisfy.

## 3. Set up the directory

Lay the directory out with the scaffold. It also lists the parliament in the table of implemented parliaments in the [README](../README.md) (parliaments in production first, then the others by code), and creates a local data repository beside this one, with the layout from section 6 and the status line `in development`:

```bash
python -m optv.scripts.readiness scaffold <CODE> --name "<Parliament Name>" \
    --type <national|regional|supranational> --language <iso 639-3> \
    --language-code <iso 639-1> --spacy-model <model> --period <N> \
    --media-license "<terms of use of the video recordings>" [--dry-run]
```

A parliament set up by hand needs the same three things done by hand: the directory, the README row and the data repository. `python -m optv.scripts.readiness sync` reports a parliament that is missing from the table.

The skeleton imports, parses the Conductor's arguments and passes the contract tests; its scraper, parsers and merger raise `NotImplementedError` until they are written. For what goes into them, read the implementation whose source is shaped like yours: `optv/parliaments/DE/` joins two feeds of the same speeches (a proceedings spine), a German state parliament such as `optv/parliaments/DE-BW/` hangs text onto a list of video clips (a media spine).

```
optv/parliaments/<CODE>/
├── __init__.py
├── manifest.yaml
├── workflow.py          # thin entry point — defines hooks, calls optv.shared.workflow
├── common.py            # Config class: how raw files are named and how a session's text shows; imports SessionStatus for the Conductor
├── scraper/             # fetch raw proceedings + media
├── parsers/             # parliament's native format → intermediate JSON
├── merger/              # join proceedings + media → Stage 2
└── align_prep.py        # optional — per-speech audio staging for align (adapter over optv.shared.audio_prep); only when the source needs slicing
```

The package layout matters: `workflow.py` imports from `optv.shared.*`, and the path bootstrap at the top supports both `python -m` and direct execution. Keep that bootstrap. Scraper, parser and merger modules need none: give each an `argparse` command line under `if __name__ == "__main__":` and run it as `python -m optv.parliaments.<CODE>.<package>.<module>` to exercise one part of the workflow; `tests/test_stage_cli.py` checks that every such command line starts. `optv.shared.cli` has the plumbing: `session_cli` is the whole command line of a module that works on one session, `load_config` and `setup_logging` serve the others.

## 4. Write `manifest.yaml`

Per-parliament metadata that [OpenParliamentTV-Conductor](https://github.com/OpenParliamentTV/OpenParliamentTV-Conductor) reads to know what stages this parliament supports and where to find its data. See [optv/parliaments/DE/manifest.yaml](../optv/parliaments/DE/manifest.yaml):

```yaml
name: "Deutscher Bundestag"
language: deu                   # ISO 639-3 (locale stack)
language_code: "de"             # ISO 639-1 lowercase — the Stage-2 emission code (SHORTCODES.md §3)
locale:                         # consumed by optv.shared.{align,ner}
  spacy_model: de_core_news_md          # full pip model id
  aeneas_language: deu                  # ISO 639-3 for aeneas/espeak
  entityfishing_language: de            # 2-letter for entityfishing
periods: [17, 18, 19, 20, 21]   # legislative terms covered
supported_stages: [download, parse, merge, nel, align, ner]
entity_dump_url: "https://de.openparliament.tv/api/v1/entityDump?type=all&wiki=true&exclude_document=true"
default_retry_count: 20
default_retry_delay_max: 10

# Rights metadata emitted into Stage 2 (resolved by optv.parliaments.get_rights).
# ALL FOUR rights fields — media.creator / media.license and the proceedings
# (textContents) creator / license — come from here, never from the source
# document or a code constant. Per-period overrides cover sources that change
# across electoral terms (e.g. a different corpus for older periods).
media:
  default: { creator: "Deutscher Bundestag", license: "…" }
proceedings:
  default: { creator: "Deutscher Bundestag", license: "Public Domain" }   # periods 18+ (native TEI)
  overrides:
    - periods: [16, 17]                               # ParlaMint-DE corpus
      creator: "PolMine ParlaMint-DE_beta"
      license: "CC-BY-4.0"
```

`supported_stages` lets parliaments opt out of stages that don't apply (e.g. a parliament with pre-aligned source data can omit `align`).

`ner: {corrections: true}` turns on the NER corrections ([optv/shared/ner_corrections.py](../optv/shared/ner_corrections.py)). They drop mentions on the stop list for the spaCy model's language (`optv/shared/ner_lists/stop_mentions/<lang>.txt`) and links to the Wikidata classes in `optv/shared/ner_lists/excluded_classes.yaml` (disambiguation pages, letters, name articles), and give a mention that exactly matches a faction label in `metadata/entities.json` or a speaker linked in the same sitting that entity's ID. One-off QID → QID corrections belong in the platform's EntityRemap, not in these lists; [NER-CORRECTIONS.md](NER-CORRECTIONS.md) says where a fix belongs.

`language_code` is the lowercase ISO 639-1 code the parser writes into `originalLanguage` and `textContents[].language`; read it via `optv.parliaments.get_language(parliament_id)`. It is distinct from the ISO-639-3 `language` used by the locale stack.

`locale.*` are required when `align` or `ner` is in `supported_stages`. The model name is given in full (no `lang + suffix` composition) because spaCy's naming isn't uniform across languages. Locale is injected onto `args` by `run_main` (via `optv.shared.workflow.inject_locale` → `optv.parliaments.get_locale()`) before invoking shared stages.

`readiness` names the parliament's sample sessions and holds the statements only a person can make. The pipeline does not read it; the readiness tooling does (see [section 12](#12-definition-of-done)):

```yaml
readiness:
  sample_sessions:                # session keys as Config.sessions() returns them, quoted
    - "21070"
    - "21071"
    - {id: "21072", expect_text: false, note: "why this one has no text"}
  attestations:                   # each one a statement by a person; absent = not stated
    qc_reviewed: {evidence: "<qc_evidence of the scorecard>"}
    licence_cleared: {note: "…"}
    platform_ready: {note: "…"}
    backfilled: {periods: [21]}
    live: {url: "https://…"}
  thresholds:                     # override of a default, with the reason
    speaker_wid_share: {value: 0.90, reason: "…"}
  whisper: {language: "no"}       # only when Whisper names the language differently
```

## 5. Implement Stage 1 (parliament-specific)

These three packages are where most of the parliament-specific work lives:

- **`scraper/`** — Download proceedings and media into `<data_dir>/original/{proceedings,media}/`. Send requests through `optv.shared.http.PoliteHTTP`, which spaces them and retries with backoff (`--retry-count` and `--retry-delay-max` come from the manifest). Idempotent: only fetch what's missing. Confine a run to the sessions it was asked for before the first request, by `--limit-session` or a session flag of the parliament's own: a parliament that is not live is processed for its sample sessions, and a run for three sessions must not walk a whole period at the source.
- **`parsers/`** — Convert the parliament's native format into intermediate per-session JSON. Two streams (proceedings and media) are kept separate at this point because they often need different cleanup logic.
- **`merger/`** — Join the two streams into Stage 2 JSON, one record per speech. The DE merger uses Needleman-Wunsch alignment to match transcript speech entries against media items; new parliaments can use whatever join logic the source data permits. Follow the id model: put the media id in `media.originMediaID`, the text id in `textContents[].originTextID`, and set a speech-level `originID` only for a genuine joint id — call `optv.shared.speech_id.normalize_speech_originid(speech)` at finalization to enforce this (and never put `originMediaID` at the speech top level). Build the envelope's `meta` block with `optv.shared.meta.build_meta(...)` rather than hand-assembling it: it emits the canonical, uniform meta (`schemaVersion`, `session`, dates, `processing`, `lastProcessing`/`lastUpdate`) and deliberately omits `parliament` and `electoralPeriod`, which live on every speech — not in `meta`. Set `originalLanguage` on every speech with `optv.shared.meta.fill_original_language(speeches, parliament_id)`. Emit `debug.*` keys in **camelCase** (e.g. `alignDuration`, never `align-duration`). Generic formatting helpers live in `optv.shared.merge_format`; language-specific ones (honorifics, chair→context) in `optv.shared.lang.<code>`.

Shared building blocks the parsers and the merger draw on, instead of each parliament writing its own:

| Module | For |
|---|---|
| `optv.shared.spine_merger` | the frame of a media-spine merger: `load_spine` reads the parsed media file, `save_merged` writes the merged file with its `meta` |
| `optv.shared.sequence_align` | `align_equal_keys`, the Needleman-Wunsch alignment of two key sequences (speakers, titles) |
| `optv.shared.pdf2tei` | proceedings that exist only as PDF: parsed into ParlaMint-style TEI and joined onto a media spine with `spine_join.join_text_to_spine` |
| `optv.shared.vtt` | subtitles as the text source: parse cues, merge HLS subtitle segments, turn cues into timed sentences |
| `optv.shared.sentence_split` | `split_long_sentences`, for sentences too long to align or display |
| `optv.shared.agenda_types` | `agendaItem.type` and `nativeType`: add a `classify_<code>` for the parliament's agenda titles and apply it with `annotate_agenda_item` |
| `optv.shared.confidence` | `compute_confidence`, which marks a speech whose text cannot belong to its clip (`debug.confidence` below 1) |
| `optv.shared.publish` | `save_if_changed`, which leaves a file untouched when its content did not change, so that later stages do not re-run |

A merger that can mis-join text marks the doubtful speeches rather than publishing them as sound. The readiness scorecard checks that such marks take effect where the data is imported (`samples.import_gate`) and that no unmarked speech carries more text than its clip could hold (`samples.text_plausible`).

The merger's output must validate against [stage2-full.schema.json](../optv/shared/schema/stage2-full.schema.json). Run the validator early and often:

```bash
python -m optv.shared.validators.cli --file <data_dir>/cache/merged/<session>-merged.json --schema full
```

## 6. Configure paths

`common.py` exposes a `Config` class subclassing the shared `BaseConfig` ([`optv/shared/config.py`](../optv/shared/config.py)), which defines the on-disk layout:

```
<data_dir>/
├── original/{media,proceedings}/   # raw downloads
├── cache/{merged,aligned,ner}/     # per-stage outputs
├── processed/                      # published Stage 2 files
└── metadata/                       # NEL entity dumps
```

Stage outputs go into `cache/`; `processed/` holds what the platform actually consumes. The `is_newer()` and `status()` helpers (in `BaseConfig`) drive the mtime-based "only run if input is newer than output" behaviour; parliaments that emit no transcript text set `HAS_TEXT = False` on their `Config` so `status()` skips the align/ner probes (a media-spine parliament that joins a text source onto its spine — e.g. the German state parliaments via `optv.shared.pdf2tei` — keeps `HAS_TEXT = True`). By default one speech without merged text makes the whole session `no_text`, which skips align and NER; that suits a proceedings spine, where a textless speech means an incomplete merge. A media-spine parliament that keeps clips without matching text as media-only records (AT, ES, SE, FI, PT, TW, the German state parliaments) sets `TEXT_PROBE = "any"`, so only a session without any text is `no_text`. Merged text is recognised by `debug.proceedingIndex`; a merger that grafts text without that stamp sets `TEXT_SIGNAL = "textContents"` (SE, FI, PT, TW). If the parliament needs additional directories or a different session-file glob, override the relevant attributes/methods in your `Config` subclass — don't hardcode paths in stage scripts.

## 7. Wire the workflow

`workflow.py` is the entry point but it is intentionally thin: stage orchestration (merge → NEL → align → NER → publish), the common argparser, lockfile handling, locale injection and the publish helper all live in [`optv/shared/workflow.py`](../optv/shared/workflow.py). The per-parliament file only contains the genuinely parliament-specific adapters and any extra CLI flags.

Define the hook functions, pass them as a `WorkflowHooks` instance, and call `run_main` — it handles the common argparser, logging, locale + manifest-default injection, the lockfile, and `run_workflow`:

```python
from optv.shared.align import align_audiofile
from optv.shared.workflow import WorkflowHooks, adapt_merge, run_main
from .common import Config
# parliament-specific imports
from .scraper.fetch_proceedings import download_proceedings
from .parsers.proceedings2json import parse_proceedings_directory
from .parsers.media2json import parse_media_directory
from .merger.merge_session import merge_session

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args): ...      # body of --download-original
def _parse(config, args): ...         # called after download (always)
_merge = adapt_merge(merge_session)   # merge_session(session, config, args) -> path of the merged cache file
def _align(config, session, args):    # return path to aligned cache file
    merged_file = config.file(session, 'merged')
    aligned_file = config.file(session, 'aligned', create=True)
    align_audiofile(merged_file, aligned_file, args.lang, args.cache_dir,
                    timeout=args.align_timeout,
                    max_audio_seconds=args.align_max_audio_seconds)
    return aligned_file


HOOKS = WorkflowHooks(
    parliament_id=PARLIAMENT_ID,
    download_originals=_download,
    parse_originals=_parse,
    merge_session_to_file=_merge,
    align_session_to_file=_align,
)


def _add_arguments(parser):           # optional — only genuinely-unique flags
    parser.add_argument("--year", type=int, default=None, help="…")


def main():
    run_main(PARLIAMENT_ID, HOOKS, description="…",
             add_arguments=_add_arguments,   # omit if no extra flags
             config_cls=Config)
```

`--period` / `--lang` / `--retry-count` / `--retry-delay-max` / `--limit-session` and all stage flags are **shared** (added by `build_common_argparser`; the first four default from the manifest) — do not re-declare them. Add only flags unique to this parliament via `add_arguments`, e.g. a `--session` flag naming seed sessions for the download. The path bootstrap at the top of `workflow.py` (for `python -m` + `./workflow.py`) is still required.

Then invoking the workflow is unchanged from a user perspective — each `--*` flag enables one stage:

```bash
./optv/parliaments/<CODE>/workflow.py --period=N <data_dir> \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

Notes for filling in the hooks:

- **`_download` and `_parse`** are parliament-specific because every source publishes differently. `_parse` runs on `--download-original` and `--merge-speeches` runs (nothing downstream of merge reads its output); gate any expensive work on mtime checks inside the hook.
- **`_merge`** is the per-session merger call. The shared runner handles the `is_newer` check, the demotion guard, and the publish — your hook just produces the merged cache file and returns its path. For a merger with the signature `merge_session(session, config, args) -> Path`, `adapt_merge(merge_session)` is the whole hook.
- **`_align`** receives `(config, session, args)` and returns the aligned cache file path. There are three established shapes, in increasing factoring:
  - **Per-speech audio already cached** (DE's shape): a hand-written `_align` calling `align_audiofile` is enough — nothing to slice.
  - **Session/debate stream that must be sliced per speech** (SE/EU/FR/FI/NO/PT): put the parliament's field mapping in an `align_prep.py` adapter over `optv.shared.audio_prep` — the shared `prepare_per_speech_audio` driver owns download-once / slice / cache — and build the hook with `make_align_hook(prepare_per_speech_audio)` instead of hand-writing `_align`.
  - **`#t=start,end` media-fragment family** (the DE state parliaments): no adapter file is needed — `make_align_hook(make_fragment_prepare(hls=…))` in `workflow.py` is the whole hook.
- **`session_in_scope=(args, session) -> bool`** is optional; the default is `session.startswith(str(args.period))`. Override only if your session keys don't have that shape; `period_prefix_scope(width=…, sep=…)` covers zero-padded periods (`08025`) and keys with a separator after the period (`22_11518`).
- **NEL, NER, the publish step, `--update-nel-entities`, the lockfile, validation** — all already shared; you do not re-implement any of them. The publish helper carries already-published wids and per-speech enrichments forward, so a stale worker cannot silently strip data a newer worker had produced.

## 8. External assets

- **Wikidata entity dump** — `metadata/entities.json`, mapping known speaker names + electoral periods to Wikidata QIDs; the NEL stage links against it. A parliament with a platform instance fetches it from the URL in `manifest.yaml` (`entity_dump_url`). One without builds it from Wikidata: `python -m optv.shared.entity_dump_bootstrap <CODE> <data_dir>`, configured by an `entity_bootstrap` block in the manifest (the Wikidata item of "member of this parliament" and the parties as the source writes them; see the module's docstring and a German state parliament's manifest).
- **Entity-fishing API endpoint** — required by the NER stage. Pass via `--ner-api-endpoint`. Public instances exist; for production, run your own.
- **`ffmpeg` and `espeak`** — required by the alignment stage (aeneas dependency).
- **A spaCy model for the parliament's language** — declared in `manifest.locale.spacy_model` (single source of truth). Install via `python -m spacy download <model>`.

## 9. Verify end-to-end

1. Pick three sample sessions and run the full workflow on them, scoped with `--limit-session <regex>` or the parliament's own session flag. A parliament that is not live is processed for its sample sessions only: alignment and NER of a whole period take days and wait until the samples are cleared.
2. Inspect the published file in `<data_dir>/processed/`.
3. Run the validator over the whole processed directory:

   ```bash
   python -m optv.shared.validators.cli --dir <data_dir>/processed --schema full
   ```

4. Spot-check sentence timings, Wikidata IDs on a known speaker, and entity extraction quality.
5. Declare the sample sessions in the manifest (`readiness.sample_sessions`) and score them:

   ```bash
   python -m optv.scripts.readiness check <CODE>
   ```

Add tests for the parliament's parsers and merger under `tests/` (see the existing `tests/test_<code>*.py` files). `tests/test_conductor_contract.py` picks a new parliament up automatically and checks the surface the Conductor depends on.

## 10. Write the parliament README

Every parliament directory carries a `README.md` covering the things an operator needs to run and reason about that pipeline. It is **distinct from research/background notes** (which live in a separate uncommitted research repo) — keep this file focused on the implementation that is checked in here.

Use this skeleton:

```markdown
# <Parliament Name> (<CODE>)

<1–2 sentence intro: what this directory implements + a link back to
docs/ADDING-A-PARLIAMENT.md for repo-wide context and to
Architecture/DATA-STRUCTURES.md for how this parliament's data shape fits the
cross-parliament model.>

## Data model

<The two input streams. Name the scraper module → parser module → on-disk
location for each. State which stream is the authoritative spine
(proceedings-spine like DE/DE-RP, media-spine like ES/SE, or timestamp-join
like EU). Mention any parliament-specific preprocessing (e.g. EU's per-sitting
HLS slicing in `align_prep.py`).>

## Merge strategy

<The merger algorithm and join key: Needleman-Wunsch on speaker+title (DE,
DE-RP), surname-sequence walk-out (ES), integer `anforande_nummer` (SE),
timestamp-window (EU). One paragraph, link the file path.>

## Running

<The `workflow.py` invocation with the `--period` value and the stage flags. Document the parliament-specific flags (`--session`, `--year`, `--kid`, …).>

## Access notes (optional)

<Auth, WAF/Cloudflare bypass, manual delivery channels — only when there's
something a new operator would trip over. Skip for plain public feeds.>

## Known limitations

<Bulleted, operator-facing. Each bullet states the limitation, why it exists,
and any `debug.*` signal it surfaces. Examples: scope (one period or chamber
only), data-quality gaps, missing live scraper, aspirational
`entity_dump_url`, alignment quirks.>
```

Do not duplicate content that already lives elsewhere: pipeline concepts go in [Architecture/PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md), the Stage 2 format in [Architecture/STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md), cross-parliament structural divergences in [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md), the generic onboarding flow in this file, and per-parliament metadata (spaCy model, language codes, `supported_stages`, `entity_dump_url`) in `manifest.yaml`. The README links to those rather than restating them.

### The entry in DATA-STRUCTURES.md

The README's link to DATA-STRUCTURES.md needs an entry to arrive at, so a new parliament is added to that document in the Architecture repository as part of this step:

- a subsection in its section 2, in the group the parliament belongs to (supranational, national, regional), after the last entry of that group;
- the parliament in every list and table of its sections 3 and 4 that names parliaments: the ID slot widths, the media-per-speech variants, the period encodings, what a session is, the two-level periods, the person IDs, and which streams carry the speeches;
- the day of the change in the `Last updated` line of its header.

The document's introduction says what belongs there, and the existing entries show the form. An entry states the shape of the parliament's data where it differs from the Bundestag baseline: the temporal hierarchy and how it maps to `electoralPeriod.number` and `session.number`, the ID model, how media relates to speeches, the agenda granularity, and what kind of join brings the sources together. It names the parliaments that share a trait. Scraping mechanics, operating notes, data quality and licence questions stay in the parliament's README; field definitions are in STAGE2-FORMAT.md.

Write the entry once sample sessions are published, from the published files and the implementation, so that it states what the output is. Like every document here it describes the current state, without history, and the header line holds its only date. When a later change alters a parliament's data shape, its entry and that date change with it.

## 11. Onboard with Conductor

Once the parliament works standalone, add it to the Conductor's `config/parliaments.yaml` so it appears in the web UI; the scaffold prints the block to add. The Conductor reads that file when it starts, so restart it afterwards, and a deployment that mounts each data repository separately needs the new one mounted too. See the [Conductor README](https://github.com/OpenParliamentTV/OpenParliamentTV-Conductor) for the deployment configuration format.

## 12. Definition of done

A parliament is done in steps, and [READINESS.md](READINESS.md) shows for every parliament which step it has reached and what keeps it from the next. The page is generated from one scorecard per parliament (`docs/readiness/<CODE>.json`); its criteria table says what each criterion checks.

| Level | Name | What it takes |
|---|---|---|
| 1 | implemented | the directory from section 3 exists |
| 2 | samples pass | the sample sessions went through every supported stage and satisfy the level 2 criteria: valid Stage 2, complete import fields, speaker IDs and sentence timing above their thresholds, plausible text, rights on every entry, and the implementation has tests |
| 3 | QC cleared | Whisper transcripts of the sample sessions confirm that the published text is what was said, a person reviewed the reports, and licence and platform fit are attested |
| 4 | backfilled | the periods to be published are processed in full, declared in the manifest |
| 5 | live | a platform instance serves the data, declared in the manifest |

Level 3 is the bar for production: from there a backfill and going live are a matter of running them. Until then the parliament stays `in development`.

```bash
python -m optv.scripts.readiness check <CODE>      # score, rewrite the scorecard and READINESS.md
python -m optv.scripts.readiness qc <CODE>         # compare text and transcript where a transcript exists
python -m optv.scripts.readiness estimate <CODE>   # plan the outstanding transcription, with its duration
```

`check` reads local files only and takes seconds. Whisper evidence costs about 0.8 hours of computing per hour of audio, so it is produced by a detached job: `estimate` prints the plan and a start command that carries the plan's token, `start` runs it in the background, `jobs` reports on it. A job transcribes the audio clips the align stage cached for the sample sessions and downloads nothing; it needs `pip install -r requirements-qc.txt` and the Whisper model in the local Hugging Face cache.

Processing the sample sessions themselves, with the downloads alignment needs, runs as a job too once the manifest declares how:

```yaml
readiness:
  sample_run:
    - sessions: ["21070", "21071", "21072"]
      args: ["--period=21", "--limit-session", "^2107[012]$"]
```

Each entry is one workflow invocation: the sample sessions it covers and the arguments that confine it to them, downloads included. Declare it after reading the parliament's download hook and confirming the arguments hold there; the stages are added by the tooling, and an entry names none. `python -m optv.scripts.readiness estimate <CODE> --sample-run` then plans the run. Its duration is estimated from earlier sample runs on record, and the plan says so when there are none.

```bash
python -m optv.scripts.readiness sync [--apply]    # README table, data repository READMEs, Research profiles
```

`sync` compares the status entries kept by hand with the scorecards and, with `--apply`, rewrites the values that differ. A parliament without a row in the README table is reported for a person to add.
