# OPTV Stage 2 Schema

JSON Schema (Draft-07) definitions for the OpenParliamentTV Stage 2 session format — the canonical shape produced by the merger and consumed by the platform importer.

- [`stage2-minimal.schema.json`](stage2-minimal.schema.json) — the minimum fields required for platform import.
- [`stage2-full.schema.json`](stage2-full.schema.json) — the canonical full shape; supersets the minimal schema and describes every field the pipeline emits.

For the human-readable format spec (envelope, field categories, text and video modes), see [Architecture/STAGE2-FORMAT.md](https://github.com/OpenParliamentTV/OpenParliamentTV-Architecture/blob/main/STAGE2-FORMAT.md). Real-world examples are in [../docs/EXAMPLES/](../docs/EXAMPLES/).

---

## Which schema should I use?

| Situation | Use |
|-----------|-----|
| Validating incoming data at a system boundary (e.g. accepting uploads from a new parliament) | **minimal** |
| Validating post-merge pipeline output | **full** |
| Verifying a writer produces well-formed output | **full** |
| Deciding whether a file is importable | **minimal** |

Both schemas use `"additionalProperties": true` at every object level so the corpus can evolve without breaking validation. The full schema fails a file only when a field is actively malformed (e.g. a datetime that does not match the ISO 8601 pattern).

---

## Conventions

### `meta.schemaVersion`

Optional. New writers SHOULD include `"schemaVersion": "1.0"`. Readers MUST treat an absent value as `"1.0"`.

### Datetime pattern

```
^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$
```

- Fractional seconds allowed
- Timezone optional (some older files lack one)
- `Z` accepted as UTC marker

### Wikidata IDs

```
^(Q\d+)?$
```

Empty strings are allowed (= unresolved). The semantic validator emits a warning for empties and missing `wid` fields. Use the `wikidataIdNullable` definition where `null` is also valid (NER output).

### Numeric-string timestamps

`textContents[].textBody[].sentences[].timeStart` and `timeEnd` are strings that look like floats (`"1.000"`). The pattern is `^\d+(\.\d+)?$`.

### `people[].context` — valid values

```
main-speaker
speaker
president
vice-president
main-proceeding-speaker
interim-president
Unknown
```

`Unknown` is accepted by the schema but flagged as a data-quality warning by the semantic validator.

### `agendaItem.speechIndex` — deprecated

Legacy period-17 artefact (nested speech index). New writers MUST use top-level `data[].speechIndex`. Accepted by the full schema; the semantic validator warns.

---

## What the schemas do NOT cover

Schema validation is structural. Cross-field, cross-item, and cross-source invariants live in [../validators/semantic_validator.py](../validators/semantic_validator.py):

1. Parliament code is in the known-parliaments set.
2. `dateEnd >= dateStart` at meta, speech, and sentence levels.
3. `speechIndex` is sequential, 1-indexed, and unique within a session.
4. `sentences[].timeStart < timeEnd` (cast to float).
5. `textContents[].sourceURI` is a URL, not a local filesystem path.
6. Warnings when `people[].wid` or `people[].faction` are absent (faction warning is restricted to speaker contexts where a faction is expected).
7. Warning on the deprecated `agendaItem.speechIndex`.
8. Warning on `Unknown` speaker context.
9. Warning when two speeches in a session share a `media.sourcePage` (`semantic.media.sourcePage.duplicate`). The platform's media.php import keys speech identity on `sourcePage`, so duplicates collapse distinct speeches at import. Parliaments serving one video per session/debate/part must make it unique per speech — e.g. appending the per-speech start offset (SE `?pos=`, NO `&t=`, FI `?start=`) or a per-speech id (DE-SH, DE-ST `?player=`).
10. Warning on a deprecated speech-level `originTextID` (`semantic.speech.originTextID_deprecated`) — the speech id belongs in `originID` (joint id) or `textContents[].originTextID`. See [Speech identity model](#speech-identity-model-originid--originmediaid--origintextid).
11. Warning when `originalLanguage` matches no `textContents[].language` (`semantic.speech.originalLanguage_mismatch`) — they must share the same code standard so `originalLanguage` can select the original text.
12. Warning when `meta` carries `parliament` or `electoralPeriod` (`semantic.meta.duplicatesPerSpeech`) — those live per-speech in `data[]`; a meta-level copy is redundant.
13. Warning on a speech-level `originMediaID` (`semantic.speech.originMediaID_misplaced`) — the media source id belongs in `media.originMediaID`. See [Speech identity model](#speech-identity-model-originid--originmediaid--origintextid).
14. Warning when a speech with `textContents` lacks `debug.proceedingIndex` (`semantic.debug.proceedingIndex.missing`) — `Config.status()` reads that key to detect merged text.

The merger sources all four rights fields (`media`/`textContents` `creator`+`license`) from the parliament's `manifest.yaml` via `optv.parliaments.get_rights()` — never from the source document or a code constant. `meta` is assembled by `optv.shared.meta.build_meta()`; `debug.*` keys are camelCase.

---

## Running the validator

```bash
python -m optv.shared.validators.cli --dir <data_dir>/processed --schema full
python -m optv.shared.validators.cli --file <session>.json --no-semantic
```

Schema errors do not block publishing in the workflow; findings are logged for review.

## Language codes & multilingual text (convention)

Stage-2 language codes use **ISO 639 Alpha-2, lowercase** (`de`, `es`, `sv`, `fr`,
`pt`, `nb`, `zh`, `en`; Alpha-3 lowercase only in special cases) per
`OpenParliamentTV-Architecture/SHORTCODES.md` §3. Each parliament pins its code in
`manifest.yaml` as `language_code`, read via `optv.parliaments.get_language()`.

`textContents[]` is an array with a per-entry `language`. When a parliament exposes
the same speech in several languages (e.g. the EU CRE in 24 languages), emit **one
`textContents[]` entry per language**; the entry whose `language == originalLanguage`
is the original, the rest are translations/interpretations. So `originalLanguage`
and every `textContents[].language` MUST use the same code standard — the semantic
validator (`semantic.speech.originalLanguage_mismatch`) warns when they diverge.
Multilingual **media** (interpretation booths / sign-language tracks) is out of
scope for now; `media` stays a single object.

## Speech identity model (originID / originMediaID / originTextID)

Three id slots, each at its own level, no duplication:

- `speech.originID` — a **joint** speech id, set **only** when the source has one
  identity spanning media ⋈ proceedings (e.g. SE's `anforande`-based key,
  `HD0930-1`). Absent when there is no joint id (DE, EU, FR, …): the speech is
  identified by its text id + `speechIndex`.
- `media.originMediaID` — the media source id (platform `MediaOriginMediaID`).
- `textContents[].originTextID` — the proceedings/text source id (platform
  `TextOriginTextID`).

Mergers call `optv.shared.speech_id.normalize_speech_originid` at finalization:
it promotes a legacy speech-level `originTextID` to `originID`, then drops
`originID` when it merely repeats the media or a text id. The platform does not
read the speech-level id at all (media identity = `sourcePage`).

## `debug` field contract

`debug` is an open object (`additionalProperties: true`) — parlers may stash
arbitrary provenance there. Only a small set is **load-bearing** (read
downstream); everything else is write-only breadcrumbs that aid auditing a bad
merge and are cheap to keep.

Load-bearing keys — never drop or rename without updating every reader in
lockstep:

| Key | Reader |
| --- | --- |
| `debug.confidence`, `debug.linkedMediaIndexes` | Platform import gate (`media.php`) + Conductor |
| `debug.confidenceReason` | `publish.carry_forward_enrichments` |
| `debug.alignDuration` | `Config.status` / `publish.data_has_timing` / Conductor |
| `debug.nerDuration` | `Config.status` / `publish.data_has_ner` / Conductor |
| `debug.proceedingIndex` | `Config.status` (text-merged signal) |
| `debug.mediaIndex`, `debug.proceedingIndexes` | intra-merge linking |
| `debug.transcriptSpeakerId`, `debug.transcriptCHash` | DE-ST merger (transcript fetch) |

Everything else (`rednerRaw`/`gruppeRaw`/… raw speaker/faction strings,
`originalTitle`, `proceedingsSource`, `alignError`, per-source ids, …) is
**provenance**: not read by the pipeline or platform, retained deliberately
because (a) raw speaker/faction strings are the documented way to audit a wrong
NEL/merge join and (b) `alignError` is a real per-speech alignment-failure
diagnostic. A blanket strip was considered and rejected — the keys are unread
but harmless, and several are conditionally useful. New stages should add a
distinguishing `debug.*` key and, if it gates a status, teach `Config.status`
about it.
