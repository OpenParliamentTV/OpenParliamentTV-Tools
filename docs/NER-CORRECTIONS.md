# NER corrections: where a fix belongs

A wrong or missing entity in a speech can be fixed in four places: two lists in this repo, the platform's EntityRemap, and the platform's entity table. This page says which one to use.

## How an entity reaches the platform

1. **spaCy** (the parliament's `locale.spacy_model`) tags spans as entities. Only these spans can become entities.
2. **entity-fishing** links each span to a Wikidata QID in its sentence. Spans it cannot link are sent again as bare terms without the sentence, which is where many wrong links come from. Each link carries a score.
3. **NER corrections** ([optv/shared/ner_corrections.py](../optv/shared/ner_corrections.py)), where the parliament's manifest sets `ner: {corrections: true}`:
   - a mention on the stop list for the spaCy language is dropped;
   - a mention that exactly matches a faction label or alias in `metadata/entities.json`, or a speaker NEL linked in the same sitting, gets that entity's QID, also where entity-fishing linked nothing;
   - a mention still without a QID is dropped;
   - a link whose Wikidata class (`P31`) is on the excluded list is dropped.
4. **Stage 2** stores the result per sentence as `entities: [{label, wid, wtype, score}]`.
5. **Platform import** (`modules/import/media-writer.php` in OpenParliamentTV-Platform):
   - the **EntityRemap** rewrites the QID (a parliament row wins over a global row);
   - if the QID is in the platform's **entity table**, the entity becomes an annotation; otherwise it becomes an **entity suggestion** for curation.

The score is stored as the annotation's confidence; it never decides whether an entity is kept. Speakers (`people[]`) take a separate path: NEL links them against `metadata/entities.json`, the EntityRemap does not apply to them, and a speaker QID the entity table lacks becomes a suggestion.

For a parliament whose entity list comes from the platform (`entity_dump_url`, e.g. DE), `metadata/entities.json` is a copy of the platform's entity table, so its names also reach step 3. The platform fills that table from Wikidata through the Additional Data Service (ADS), which overwrites edits made in the platform: entity names and aliases are corrected in Wikidata only.

## Deciding where a fix goes

Ask in this order and stop at the first yes.

| Question | Fix | Where |
|---|---|---|
| Is the mention never an entity in any text of this language (a form of address, a role noun, a fragment such as "BÜNDNIS")? | Add it to the stop list | `optv/shared/ner_lists/stop_mentions/<lang>.txt` |
| Is the link a kind of page that is never a valid entity in any parliament (disambiguation page, given or family name, letter, list)? | Add the class (the target's `P31`) if it is not listed yet | `optv/shared/ner_lists/excluded_classes.yaml` |
| Is the right entity a faction of this parliament, or a speaker in that sitting? | Give the entity that name in Wikidata (an alias); it reaches the platform through the ADS and the entity list through the entity dump or the parliament's entity-dump builder. For a speaker, fix the NEL link | Wikidata |
| Is "QID X is wrong, QID Y is right" true for every mention of X in this parliament (or in every parliament)? | Add a remap row | Platform EntityRemap (parliament row, or global) |
| Is the link right, but the entity is not shown? | Approve the suggestion; this is curation, not a correction | Platform entity table |
| Is the link right for some mentions of X and wrong for others? | Neither place can fix it safely; leave it | — |

## What differs between the two places

| | Lists in this repo | Platform EntityRemap |
|---|---|---|
| Keyed by | the mention text (stop list) or the target's Wikidata class | one QID |
| Scope | a language (stop list) or every parliament (classes) | one parliament, or all |
| Changed by | a commit | an admin, in the platform's settings |
| Takes effect | when NER next runs for a session | at the next import of a session |
| Applies to | the Stage 2 output, for every consumer | the platform's NER annotations only |

The repo holds rules about a kind of error and never a single QID → QID pair; a single wrong QID is a remap row. A remap row keyed on a QID rewrites every mention linked to it, so when only one wording is wrong (e.g. "Union" for the CDU/CSU group), an alias on the right entity in Wikidata is the narrower fix.

## Examples

| Seen | Fix |
|---|---|
| "Srs" (from "Srs. Deputados") → Sender Rewriting Scheme | Portuguese stop list |
| "Pedro Pinto" → a Wikidata disambiguation page | excluded class `Q22808320`; the right person comes from the sitting's speakers |
| "CH" → Order of the Companions of Honour | the party is in the PT entity list as faction `CH`, so step 3 links Chega |
| "AfD" → the party, where the platform tracks the Bundestag group | DE remap row party → group (step 3 does the same where corrections are on) |
| "Union" → Union (American Civil War) | alias "Union" on the CDU/CSU group's Wikidata item |
| "Euro" → Euro, filed as a suggestion | correct link; whether the platform tracks it is a curation decision |
