# Classifying suspect speeches

How to read the Whisper comparison of a session and name what went wrong in a speech. The rules are fixed here so that a review applies them, instead of fitting them to what it finds.

## The figures

`optv/shared/whisper_diff.py` computes them per speech (`compute_metrics`); `<session>-qc.json` holds one row per speech and `<session>-report.md` shows the same values with text excerpts of the five most suspect speeches.

| Field | Meaning |
|---|---|
| `chars_proceedings` | characters of the published text, in the language spoken |
| `chars_whisper` | characters of the transcript |
| `cps_proceedings` | published characters per second of aligned audio; speech runs at roughly 15 to 17 |
| `text_similarity` | 0 to 1, similarity of the normalised texts; `null` without a transcript |
| `text_inflation_ratio` | `chars_proceedings / chars_whisper` |
| `boundary_divergence_s` | largest gap between where the transcript and the aligned sentences start or end |
| `speaker_change_count` | speaker changes heard in the clip, when the job ran with the speaker check |
| `n_speech_ids_attached` | distinct source speeches merged into this one, where the merger records them |
| `suspect_score` | weighted sum of the above; the higher, the more worth a look |

A faithful speech has a similarity of 0.8 to 0.95. It is rarely higher, because the record is edited: interjections are added, slips removed.

## Which speeches to classify

Every speech with `suspect_score > 10`, plus every speech with `text_similarity < 0.5` and more than 200 published characters. Below that the signal is mostly interjection noise.

## The classes

Give each such speech exactly one class, the first that matches.

| # | Class | Trigger | Likely cause |
|---|---|---|---|
| 1 | `tail-accumulation` | `text_inflation_ratio > 3`, and the published text ends with the chair closing the item (the debate is closed, the vote is called) | the last speech of an agenda item absorbed the speakers before it and the chair's closing |
| 2 | `text-inflation` | `text_inflation_ratio > 3` on more than 800 published characters | the merger put many source speeches, a question time or a whole debate, onto one clip |
| 3 | `text-misassignment` | `text_similarity < 0.5` while `0.5 < text_inflation_ratio < 2` | the wrong passage was paired with this clip: similar length, different content |
| 4 | `media-misalignment` | `boundary_divergence_s > 30` | the clip was cut or aligned at the wrong place |
| 5 | `interjection-noise` | `speaker_change_count > 5` and `text_similarity > 0.8` | the speech is fine; interjections and chair remarks raise the score |
| 6 | `empty-audio` | no transcript text and no transcript duration | the clip is missing or silent |
| 7 | `transcription-only` | fewer than 200 published characters and `text_similarity < 0.5` | a short or ceremonial utterance where the transcript itself may be off |

Telling 1 from 2 needs a look at the end of the published text in the report. Inflation from a question time shows question and answer interleaved through the middle instead.

Classes 1 to 4 are defects of the data. Classes 5 and 7 are not. Class 6 is a staging problem.

## What reaches users

A defect matters most when the platform imports the speech with its text. `optv.scripts.readiness.speeches.would_import_text` mirrors the importer: text is imported unless the speech carries both `debug.confidence` and `debug.linkedMediaIndexes` and fails the check on them. Report every finding with that flag. A parliament whose merger never lowers the confidence, or sets it without `linkedMediaIndexes`, has no protection at all: every defect imports.

Where the agenda item has a `type`, group the findings by it. Question times and government questionings behave differently from debates, and a pattern confined to one type usually points at one rule in the merger.

## Reporting

Per parliament: the number of speeches in each class, how many of them would import with text, the sessions and speech indexes of the defects, and the one or two mechanisms that explain most of them. Say when a class has too few cases to carry a conclusion.
