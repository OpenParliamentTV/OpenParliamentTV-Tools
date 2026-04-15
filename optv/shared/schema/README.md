# OPTV Stage 2 Schema

JSON Schema (Draft-07) definitions for the OpenParliamentTV Stage 2 session format — the canonical shape that comes out of the merger and is consumed by the platform importer.

- [`stage2-minimal.schema.json`](stage2-minimal.schema.json) — the minimum fields required for platform import. Every field here is present in 100% of real OpenParliamentTV-Data-DE files.
- [`stage2-full.schema.json`](stage2-full.schema.json) — the canonical full shape. Supersets the minimal schema and describes every field observed across the 1,034-file Data-DE corpus as of 2026-04-15.

See [../../../_planning/stage2-discrepancy.md](../../../_planning/stage2-discrepancy.md) for the field-by-field derivation.

---

## Which schema should I use?

| Situation | Use |
|-----------|-----|
| Validating incoming data at a system boundary (e.g., accepting uploads from a new parliament) | **minimal** |
| Validating post-merge pipeline output | **full** |
| Verifying a writer produces well-formed output | **full** |
| Deciding whether a file is importable | **minimal** |

Both schemas use `"additionalProperties": true` at every object level so that the corpus can evolve without breaking validation. The full schema fails a file only when a field is actively malformed (e.g., a datetime that does not match the ISO 8601 pattern).

---

## Conventions

### `meta.schemaVersion`

**Not in any existing file.** New writers SHOULD include `"schemaVersion": "1.0"`. Readers MUST treat an absent value as `"1.0"`. Both schemas mark the field optional.

### Datetime pattern

```
^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$
```

- Fractional seconds allowed
- Timezone optional (many real values from early periods lack one)
- `Z` accepted as UTC marker

### Wikidata IDs

```
^(Q\d+)?$
```

Empty strings are allowed (= unresolved). The semantic validator emits a warning for empties and missing `wid` fields. Use the `wikidataIdNullable` definition where `null` is also valid (NER output).

### Numeric-string timestamps

`textContents[].textBody[].sentences[].timeStart` and `timeEnd` are strings that look like floats (`"1.000"`). The pattern is `^\d+(\.\d+)?$`.

### `people[].context` — 7 valid values

The format spec lists 4, but real data has 7:

```
main-speaker, speaker, president, vice-president,
main-proceeding-speaker, interim-president, Unknown
```

`Unknown` is accepted by the schema but flagged as a data-quality warning by the semantic validator.

### `agendaItem.speechIndex` — deprecated

Legacy period-17 artifact (nested speech index). New writers MUST use top-level `data[].speechIndex`. Accepted by the full schema; semantic validator warns.

---

## What the schemas do NOT cover

Schema validation is structural. Cross-field, cross-item, and cross-source invariants are the job of `optv/shared/validators/semantic_validator.py`. Rules include:

1. Parliament code in known-parliaments config
2. `dateEnd >= dateStart` (at meta, speech, sentence levels)
3. `speechIndex` sequential and 1-indexed with no duplicates
4. `sentences[].timeStart < timeEnd` (cast to float)
5. `textContents[].sourceURI` is a URL, not a local filesystem path
6. `people[].wid` and `people[].faction` warnings when absent
7. Deprecated `agendaItem.speechIndex` warning
8. `Unknown` speaker context warning

See [stage2-discrepancy.md §Semantic-validator rules](../../../_planning/stage2-discrepancy.md#semantic-validator-rules-output-of-this-phase) for the full list.

---

## Current pass rates (2026-04-15)

Run via [`_planning/phase_b_validate.py`](../../../_planning/phase_b_validate.py):

| Schema | Passed | Total | Notes |
|--------|--------|-------|-------|
| minimal | 1,034 | 1,034 (100%) | One file (17003) lacks `data[31].people`; schema does not require per-speech `people` |
| full | 1,027 | 1,034 (99.3%) | 7 files (18013-18018, 18023) have corrupt `meta.dateStart`/`dateEnd` strings — tracked as data-repair tasks, not schema work |

See [stage2-discrepancy.md §Known data-corruption bugs](../../../_planning/stage2-discrepancy.md#known-data-corruption-bugs).

---

## Changelog

### 1.0 — 2026-04-15
- Initial schema derived from 1,034 Data-DE sessions spanning electoral periods 17–21.
