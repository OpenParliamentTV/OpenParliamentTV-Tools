# Adding a parliament

This is the end-to-end checklist for onboarding a new parliament into OpenParliamentTV. The German Bundestag implementation in [optv/parliaments/DE/](../optv/parliaments/DE/) is the reference; new parliaments follow the same shape.

For the data contract and pipeline concepts, see [Architecture/STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md) and [Architecture/PIPELINE.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/PIPELINE.md). For how parliaments' data shapes differ structurally, see [Architecture/DATA-STRUCTURES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/DATA-STRUCTURES.md).

---

## 1. Choose a shortcode

Follow [Architecture/SHORTCODES.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/SHORTCODES.md). National parliaments use the country's ISO 3166 alpha-2 code (`DE`, `SE`, `FR`); regional parliaments use the ISO subdivision code (`DE-BE`, `DE-BY`); custom codes are allowed where ISO doesn't fit (`US-NYC`, `CAT`). Always uppercase.

The shortcode becomes the directory name (`optv/parliaments/<CODE>/`) and the data-repo suffix (`OpenParliamentTV-Data-<CODE>`).

## 2. Survey the source data

Before writing any code, confirm the parliament publishes the four things the pipeline needs:

- **Video** for every speech, fetchable by URL. Either per-speech files or a session video plus per-speech timecodes. Without accessible video, the parliament is not a viable target.
- **Proceedings / transcripts** in some structured format (TEI XML, ParlaMint, Akoma Ntoso, custom JSON, etc.).
- **A media metadata feed** (RSS, JSON API, …) listing video items with at least the parliament's internal media ID and a public source page.
- **Wikidata coverage** for at least the major speakers. Not strictly required (the NEL stage tolerates gaps), but coverage gaps degrade platform features.

Validation target for the eventual output: [optv/shared/schema/stage2-full.schema.json](../optv/shared/schema/stage2-full.schema.json). Read [optv/shared/schema/README.md](../optv/shared/schema/README.md) before designing the parser — it documents enum values, datetime patterns, and conventions that the parser must satisfy.

## 3. Set up the directory

Copy `optv/parliaments/DE/` as a template:

```
optv/parliaments/<CODE>/
├── __init__.py
├── manifest.yaml
├── workflow.py          # thin entry point — defines hooks, calls optv.shared.workflow
├── common.py            # Config class, paths, file naming (re-exports shared SessionStatus + publish helpers)
├── scraper/             # fetch raw proceedings + media
├── parsers/             # parliament's native format → intermediate JSON
├── merger/              # join proceedings + media → Stage 2
├── Makefile             # convenience targets driven by file mtimes
└── update               # shell wrapper baking in default flags
```

The package layout matters: `workflow.py` imports from `optv.shared.*`, and the path bootstrap at the top supports both `python -m` and direct execution. Keep that bootstrap.

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
entity_dump_url: "https://de.openparliament.tv/data/entity-dump/?type=all&wiki=true&exclude_document=true"
default_retry_count: 20
default_retry_delay_max: 10

# Rights metadata emitted into Stage 2 (resolved by optv.parliaments.get_rights).
# Only the literal values that would otherwise be code constants; data-driven
# fields (e.g. a creator pulled from the source document) stay in the parser.
# Per-period overrides cover sources that change across electoral terms.
media:
  default: { creator: "Deutscher Bundestag", license: "…" }
proceedings:
  default: { license: "Public Domain" }              # periods 18+ (native TEI)
  overrides:
    - periods: [16, 17]                               # ParlaMint-DE corpus
      creator: "PolMine ParlaMint-DE_beta"
      license: "CC-BY-4.0"
```

`supported_stages` lets parliaments opt out of stages that don't apply (e.g. a parliament with pre-aligned source data can omit `align`).

`language_code` is the lowercase ISO 639-1 code the parser writes into `originalLanguage` and `textContents[].language`; read it via `optv.parliaments.get_language(parliament_id)`. It is distinct from the ISO-639-3 `language` used by the locale stack.

`locale.*` are required when `align` or `ner` is in `supported_stages`. The model name is given in full (no `lang + suffix` composition) because spaCy's naming isn't uniform across languages. Locale is injected onto `args` by `run_main` (via `optv.shared.workflow.inject_locale` → `optv.parliaments.get_locale()`) before invoking shared stages.

## 5. Implement Stage 1 (parliament-specific)

These three packages are where most of the parliament-specific work lives:

- **`scraper/`** — Download proceedings and media into `<data_dir>/original/{proceedings,media}/`. Handle pagination, rate limits, and transient failures (the DE implementation uses `--retry-count` because the Bundestag media server returns frequent 503s). Idempotent: only fetch what's missing.
- **`parsers/`** — Convert the parliament's native format into intermediate per-session JSON. Two streams (proceedings and media) are kept separate at this point because they often need different cleanup logic.
- **`merger/`** — Join the two streams into Stage 2 JSON, one record per speech. The DE merger uses Needleman-Wunsch alignment to match transcript speech entries against media items; new parliaments can use whatever join logic the source data permits. Follow the id model: put the media id in `media.originMediaID`, the text id in `textContents[].originTextID`, and set a speech-level `originID` only for a genuine joint id — call `optv.shared.speech_id.normalize_speech_originid(speech)` at finalization to enforce this. Generic formatting helpers live in `optv.shared.merge_format`; language-specific ones (honorifics, chair→context) in `optv.shared.lang.<code>`.

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

Stage outputs go into `cache/`; `processed/` holds what the platform actually consumes. The `is_newer()` and `status()` helpers (in `BaseConfig`) drive the mtime-based "only run if input is newer than output" behaviour; video-only parliaments set `HAS_TEXT = False` on their `Config` so `status()` skips the align/ner probes. If the parliament needs additional directories or a different session-file glob, override the relevant attributes/methods in your `Config` subclass — don't hardcode paths in stage scripts.

## 7. Wire the workflow

`workflow.py` is the entry point but it is intentionally thin: stage orchestration (merge → NEL → align → NER → publish), the common argparser, lockfile handling, locale injection and the publish helper all live in [`optv/shared/workflow.py`](../optv/shared/workflow.py). The per-parliament file only contains the genuinely parliament-specific adapters and any extra CLI flags.

Define the hook functions, pass them as a `WorkflowHooks` instance, and call `run_main` — it handles the common argparser, logging, locale + manifest-default injection, the lockfile, and `run_workflow`:

```python
from optv.shared.align import align_audiofile
from optv.shared.workflow import WorkflowHooks, run_main
from .common import Config
# parliament-specific imports
from .scraper.fetch_proceedings import download_proceedings
from .parsers.proceedings2json import parse_proceedings_directory
from .parsers.media2json import parse_media_directory
from .merger.merge_session import merge_session

PARLIAMENT_ID = Path(__file__).parent.name


def _download(config, args): ...      # body of --download-original
def _parse(config, args): ...         # called after download (always)
def _merge(config, session, args):    # return path to merged cache file
    return merge_session(session, config, args)
def _align(config, session, args):    # return path to aligned cache file
    merged_file = config.file(session, 'merged')
    aligned_file = config.file(session, 'aligned', create=True)
    align_audiofile(merged_file, aligned_file, args.aeneas_language, args.cache_dir,
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

`--lang` / `--retry-count` / `--retry-delay-max` / `--session` (seed filter) / `--limit-session` and all stage flags are **shared** (added by `build_common_argparser`, defaulted from the manifest) — do not re-declare them. Add only flags unique to this parliament via `add_arguments`. The path bootstrap at the top of `workflow.py` (for `python -m` + `./workflow.py`) is still required.

Then invoking the workflow is unchanged from a user perspective — each `--*` flag enables one stage:

```bash
./optv/parliaments/<CODE>/workflow.py --period=N <data_dir> \
    --download-original --merge-speeches \
    --link-entities --align-sentences --extract-entities
```

Notes for filling in the hooks:

- **`_download` and `_parse`** are parliament-specific because every source publishes differently. `_parse` runs unconditionally after `_download`; gate any expensive work on mtime checks inside the hook.
- **`_merge`** is the per-session merger call. The shared runner handles the `is_newer` check, the demotion guard, and the publish — your hook just produces the merged cache file and returns its path.
- **`_align`** receives `(config, session, args)` and returns the aligned cache file path. For per-speech audio (DE's shape) call `align_audiofile`; for per-debate audio that needs slicing first, pre-slice into per-speech MP3s at the paths `align_audio` expects, then call it in-memory and write the result.
- **`session_in_scope=(args, session) -> bool`** is optional; the default is `session.startswith(str(args.period))`. Override only if your session keys don't have that shape (e.g. a parliament whose session strings use a separator that requires more precise prefix matching).
- **NEL, NER, the publish step, `--update-nel-entities`, the lockfile, validation** — all already shared; you do not re-implement any of them. The publish helper carries already-published wids and per-speech enrichments forward, so a stale worker cannot silently strip data a newer worker had produced.

## 8. External assets

- **Wikidata entity dump** — JSON file mapping known speaker names + electoral periods to Wikidata QIDs. URL goes in `manifest.yaml` as `entity_dump_url`. The NEL stage reads from this.
- **Entity-fishing API endpoint** — required by the NER stage. Pass via `--ner-api-endpoint` or set in `manifest.yaml`. Public instances exist; for production, run your own.
- **`ffmpeg` and `espeak`** — required by the alignment stage (aeneas dependency).
- **A spaCy model for the parliament's language** — declared in `manifest.locale.spacy_model` (single source of truth). Install via `python -m spacy download <model>`.

## 9. Verify end-to-end

1. Run the full workflow on one or two sessions with `--limit-session <regex>`.
2. Inspect the published file in `<data_dir>/processed/`.
3. Run the validator over the whole processed directory:

   ```bash
   python -m optv.shared.validators.cli --dir <data_dir>/processed --schema full
   ```

4. Spot-check sentence timings, Wikidata IDs on a known speaker, and entity extraction quality.

There is no automated test suite — validation output is the primary signal.

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

<The `update` shell wrapper if present, then the bare `workflow.py` invocation
with the `--period` value and the stage flags. Use the generic `--session`
flag to target specific seed sessions; document any parliament-specific flags
(`--year`, `--kid`, …).>

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

## 11. Onboard with Conductor

Once the parliament works standalone, add it to the Conductor's `config/parliaments.yaml` so it appears in the web UI. See the [Conductor README](https://github.com/OpenParliamentTV/OpenParliamentTV-Conductor) for the deployment configuration format.
