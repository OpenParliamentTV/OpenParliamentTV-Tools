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

---

## Running the validator

```bash
python -m optv.shared.validators.cli --dir <data_dir>/processed --schema full
python -m optv.shared.validators.cli --file <session>.json --no-semantic
```

Schema errors do not block publishing in the workflow; findings are logged for review.
