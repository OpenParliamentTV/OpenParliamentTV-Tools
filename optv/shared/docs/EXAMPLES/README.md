# Stage 2 Example Files

Real OPTV Stage 2 session documents, trimmed to 2 speeches each for compactness. All three validate against both [`stage2-minimal.schema.json`](../../schema/stage2-minimal.schema.json) and [`stage2-full.schema.json`](../../schema/stage2-full.schema.json).

| File | Source | Coverage |
|------|--------|----------|
| [`minimal.json`](minimal.json) | `17001-session.json` (period 17) | Pre-`textContents` era — only the fields required by the minimal schema. No transcripts, no NER. |
| [`with-video.json`](with-video.json) | `21002-session.json` (period 21) | `textContents` with sentence-level alignment, but no NER entities yet (recent session, NER not yet run). |
| [`full-featured.json`](full-featured.json) | `19019-session.json` (period 19) | Complete pipeline output — `textContents`, aligned sentences, NER entities with Wikidata IDs. |

Each file was produced by taking the original session and:
1. Slicing `data` to the first 2 speeches
2. (full-featured only) trimming each speech's `sentences` array to the first 3 entries

The `meta` envelope is untouched, so every field the schemas care about is exercised.

## `agendaItem.type` and `agendaItem.nativeType`

Every example carries both fields on each agenda item to demonstrate the
two-tier classification. `type` is the cross-parliament core enum from
[`optv/shared/agenda_types.py`](../../agenda_types.py) (`opening`,
`election`, `qa`, `current_affairs`, `voting`, `closing`, `regular`, …);
`nativeType` is the parliament-specific identifier preserved verbatim
(DE: ParlaMint-DE vocabulary like `DE-opening_speech`, `DE-election`;
DE-RP: `DE-RP-*`; SE: raw `kammaraktivitet`). Consumers that filter on
agenda type should prefer `type`; consumers that need the original
parliament's terminology should read `nativeType`.
