"""The criteria a parliament is scored on, each gating one level.

A criterion reads the :class:`Context` that ``scorecard.build_context`` prepares and returns a :class:`Result`. It never touches the network or the media: everything comes from the Tools tree, the published sample sessions and the Whisper evidence cached next to them.
"""

from __future__ import annotations

import importlib
import statistics
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from optv.parliaments import get_language, get_locale, list_parliaments
from optv.shared.confidence import DEFAULT_CPS_CAP_TYPES
from optv.shared.config import BaseConfig
from optv.shared.session_status import SessionStatus

from .config import Declaration, Sample
from .speeches import (debug, gate_state, has_text, missing_import_fields, sentences,
                       speech_duration, text_chars, would_import_text)

PASS = "pass"
FAIL = "fail"
NOT_EVALUATED = "not_evaluated"
NOT_APPLICABLE = "not_applicable"
STALE = "stale"
# Internal: the inputs are not on this machine. The scorecard keeps the last recorded result.
UNAVAILABLE = "unavailable"

SATISFIED = (PASS, NOT_APPLICABLE)

REQUIRED_FILES = ("manifest.yaml", "workflow.py", "common.py", "README.md")
REQUIRED_DIRS = ("scraper", "parsers", "merger")
KNOWN_STAGES = {"download", "parse", "merge", "nel", "align", "ner"}
TIME_BOUNDS_RULE = "semantic.sentence.time.bounds"


@dataclass
class Result:
    status: str
    metrics: dict = field(default_factory=dict)
    detail: str = ""


@dataclass
class SampleData:
    sample: Sample
    doc: dict | None = None            # the published session document
    status: set = field(default_factory=set)

    @property
    def speeches(self) -> list[dict]:
        return (self.doc or {}).get("data") or []


@dataclass
class SessionQC:
    """Whisper evidence of one sample session."""
    sample_id: str
    expected: set = field(default_factory=set)       # speechIndex of every text-bearing speech
    transcribed: set = field(default_factory=set)    # speechIndex with a transcript that still belongs to it
    outdated: int = 0                                # transcripts a re-merge left at the wrong index
    rows: dict | None = None                         # speechIndex -> metrics; None: no QC file
    current: bool = False                            # the QC file matches the text and the transcript
    evidence: str | None = None                      # identifies the text and transcript the figures describe


@dataclass
class Context:
    code: str
    tools: Path
    manifest: dict
    declaration: Declaration
    config: BaseConfig | None = None                 # None: no data repository on this machine
    samples: list[SampleData] = field(default_factory=list)
    whisper_available: bool = False                  # the data repository has a Whisper cache
    qc: list[SessionQC] = field(default_factory=list)
    qc_evidence: str | None = None

    @property
    def thresholds(self) -> dict:
        return self.declaration.thresholds

    @property
    def published(self) -> list[SampleData]:
        return [s for s in self.samples if s.doc is not None]


@dataclass(frozen=True)
class Criterion:
    id: str
    level: int       # the level this criterion gates
    kind: str        # "computed" or "attested"
    rule: str
    fn: Callable[[Context], Result]


def _share(part: int, whole: int) -> float | None:
    return round(part / whole, 3) if whole else None


def _listing(items: list[str], limit: int = 5) -> str:
    shown = ", ".join(items[:limit])
    return f"{shown} and {len(items) - limit} more" if len(items) > limit else shown


def _no_data() -> Result:
    return Result(UNAVAILABLE, detail="no data repository on this machine")


def _unpublished() -> Result:
    return Result(NOT_EVALUATED, detail="no sample session is published")


# ---- implementation ----

def _impl_files(ctx: Context) -> Result:
    base = ctx.tools / "optv" / "parliaments" / ctx.code
    missing = [name for name in REQUIRED_FILES if not (base / name).is_file()]
    missing += [f"{name}/" for name in REQUIRED_DIRS if not (base / name).is_dir()]
    if missing:
        return Result(FAIL, detail=f"missing: {', '.join(missing)}")
    return Result(PASS)


def _impl_manifest(ctx: Context) -> Result:
    problems = [f"readiness: {error}" for error in ctx.declaration.errors]
    try:
        get_locale(ctx.code)
    except KeyError as error:
        problems.append(str(error.args[0]))
    stages = ctx.manifest.get("supported_stages") or []
    if not stages:
        problems.append("no supported_stages")
    elif set(stages) - KNOWN_STAGES:
        problems.append(f"unknown stages: {', '.join(sorted(set(stages) - KNOWN_STAGES))}")
    if not ctx.manifest.get("periods"):
        problems.append("no periods")
    if not get_language(ctx.code):
        problems.append("no language_code")
    if problems:
        return Result(FAIL, detail="; ".join(problems))
    return Result(PASS)


def _impl_contract(ctx: Context) -> Result:
    try:
        common = importlib.import_module(f"optv.parliaments.{ctx.code}.common")
    except Exception as error:
        return Result(FAIL, detail=f"common.py does not import: {type(error).__name__}: {error}")
    config_cls = getattr(common, "Config", None)
    if not (isinstance(config_cls, type) and issubclass(config_cls, BaseConfig)):
        return Result(FAIL, detail="common.Config is not a BaseConfig")
    if not hasattr(common, "SessionStatus"):
        return Result(FAIL, detail="common.py does not export SessionStatus")
    return Result(PASS)


def own_test_modules(tools: Path, code: str, codes: list[str] | None = None) -> list[str]:
    """The test modules written for one parliament: ``tests/test_<code>.py`` and ``tests/test_<code>_*.py``."""
    def slug(value: str) -> str:
        return value.lower().replace("-", "_")

    own = slug(code)
    codes = list_parliaments() if codes is None else codes
    others = [slug(c) for c in codes if c != code and slug(c).startswith(own + "_")]
    names = []
    for path in sorted((tools / "tests").glob(f"test_{own}*.py")):
        stem = path.stem[len("test_"):]
        if stem != own and not stem.startswith(own + "_"):
            continue
        if any(stem == other or stem.startswith(other + "_") for other in others):
            continue
        names.append(path.name)
    return names


def _impl_tests(ctx: Context) -> Result:
    names = own_test_modules(ctx.tools, ctx.code)
    if not names:
        return Result(FAIL, metrics={"modules": 0},
                      detail=f"no tests/test_{ctx.code.lower().replace('-', '_')}*.py")
    return Result(PASS, metrics={"modules": len(names)})


# ---- sample sessions ----

def _samples_declared(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    minimum = ctx.thresholds["min_samples"]
    missing = [s.sample.id for s in ctx.samples if s.doc is None]
    metrics = {"declared": len(ctx.samples), "published": len(ctx.published), "minimum": minimum}
    if len(ctx.samples) < minimum:
        return Result(FAIL, metrics, f"{len(ctx.samples)} declared, {minimum} needed")
    if missing:
        return Result(FAIL, metrics, f"not published: {_listing(missing)}")
    return Result(PASS, metrics)


def missing_stages(sample: SampleData, stages: set) -> list[str]:
    """What a published sample session lacks of the ``stages`` it should have gone through."""
    missing = [stage for stage, flag in (("nel", SessionStatus.linked),)
               if stage in stages and flag not in sample.status]
    if SessionStatus.empty in sample.status:
        missing.append("speeches")
    if sample.sample.expect_text:
        if SessionStatus.no_text in sample.status:
            missing.append("text")
        else:
            missing += [stage for stage, flag in (("align", SessionStatus.aligned),
                                                  ("ner", SessionStatus.ner))
                        if stage in stages and flag not in sample.status]
    return missing


def _samples_stages(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    stages = set(ctx.manifest.get("supported_stages") or [])
    problems = []
    for s in ctx.published:
        missing = missing_stages(s, stages)
        if missing:
            problems.append(f"{s.sample.id}: no {'/'.join(missing)}")
    metrics = {"complete": len(ctx.published) - len(problems), "sessions": len(ctx.published)}
    if problems:
        return Result(FAIL, metrics, "; ".join(problems))
    return Result(PASS, metrics)


def _samples_valid(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    from optv.shared.validators import validate_stage2
    errors = bounds = warnings = 0
    rules = Counter()
    sessions = []
    for s in ctx.published:
        failing = 0
        for finding in validate_stage2(s.doc, schema="full", semantic=True):
            if finding["rule"] == TIME_BOUNDS_RULE:
                bounds += 1
                failing += 1
                rules[finding["rule"]] += 1
            elif finding["severity"] == "error":
                errors += 1
                failing += 1
                rules[finding["rule"]] += 1
            else:
                warnings += 1
        if failing:
            sessions.append(f"{s.sample.id}: {failing}")
    metrics = {"errors": errors, "time_bounds": bounds, "warnings": warnings}
    if errors or bounds:
        top = ", ".join(f"{rule} x{count}" for rule, count in rules.most_common(3))
        return Result(FAIL, metrics, f"{'; '.join(sessions)} ({top})")
    return Result(PASS, metrics)


def _samples_import_fields(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    total = 0
    problems = []
    for s in ctx.published:
        for speech in s.speeches:
            total += 1
            missing = missing_import_fields(speech)
            if missing:
                problems.append(f"{s.sample.id}#{speech.get('speechIndex')}: {', '.join(missing)}")
    metrics = {"speeches": total, "rejected": len(problems)}
    if problems:
        return Result(FAIL, metrics, _listing(problems, 3))
    return Result(PASS, metrics)


def _samples_import_gate(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    states = Counter()
    importable = 0
    leaks = Counter()
    for s in ctx.published:
        for speech in s.speeches:
            if not speech.get("textContents"):
                continue
            state = gate_state(speech)
            states[state] += 1
            if would_import_text(speech):
                importable += 1
                if state == "confidence-only" and debug(speech)["confidence"] != 1:
                    leaks[s.sample.id] += 1
    with_text = sum(states.values())
    if not with_text or not (states["gated"] or states["confidence-only"]):
        return Result(NOT_APPLICABLE, {"mode": "ungated", "text_speeches": with_text},
                      "the speeches carry no debug.confidence; the platform imports every text")
    if states["confidence-only"]:
        metrics = {"mode": "confidence-only", "text_speeches": with_text,
                   "leaks": sum(leaks.values())}
        if leaks:
            per_session = ", ".join(f"{sid}: {count}" for sid, count in sorted(leaks.items()))
            return Result(FAIL, metrics,
                          f"speeches below confidence 1 would import with text ({per_session}); "
                          "the platform ignores debug.confidence without debug.linkedMediaIndexes")
        return Result(PASS, metrics)
    return Result(PASS, {"mode": "gated", "text_speeches": with_text,
                         "importable_share": _share(importable, with_text)})


def _samples_speaker_wid(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    speeches = [speech for s in ctx.published for speech in s.speeches]
    if not speeches:
        return Result(NOT_EVALUATED, detail="the sample sessions have no speeches")
    with_wid = sum(1 for speech in speeches
                   if (speech.get("people") or [{}])[0].get("wid"))
    threshold = ctx.thresholds["speaker_wid_share"]
    share = _share(with_wid, len(speeches))
    metrics = {"share": share, "with_wid": with_wid, "speeches": len(speeches),
               "threshold": threshold}
    return Result(PASS if share >= threshold else FAIL, metrics)


def _samples_timing(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    total = timed = 0
    for s in ctx.published:
        if not s.sample.expect_text:
            continue
        for speech in s.speeches:
            for sentence in sentences(speech):
                if not sentence.get("text"):
                    continue
                total += 1
                if sentence.get("timeStart") is not None and sentence.get("timeEnd") is not None:
                    timed += 1
    if not total:
        return Result(NOT_APPLICABLE, detail="the sample sessions carry no text")
    if not timed and "align" not in (ctx.manifest.get("supported_stages") or []):
        return Result(NOT_APPLICABLE, detail="no align stage and no timed source text")
    threshold = ctx.thresholds["timing_share"]
    share = _share(timed, total)
    metrics = {"share": share, "timed": timed, "sentences": total, "threshold": threshold}
    return Result(PASS if share >= threshold else FAIL, metrics)


def _samples_text_plausible(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    min_chars = ctx.thresholds["implausible_min_chars"]
    limit = ctx.thresholds["implausible_cps"]
    checked = 0
    implausible = []
    for s in ctx.published:
        for speech in s.speeches:
            chars = text_chars(speech)
            if chars < min_chars or not would_import_text(speech):
                continue
            # A speech the merger marked is the import gate's concern. Chair text of an
            # opening, a vote or an election is long for its clip without being wrong.
            if debug(speech).get("confidence") not in (None, 1):
                continue
            agenda_type = (speech.get("agendaItem") or {}).get("type")
            if agenda_type and agenda_type not in DEFAULT_CPS_CAP_TYPES:
                continue
            duration, source = speech_duration(speech)
            if not duration or source == "text-length":
                continue
            checked += 1
            if chars / duration >= limit:
                implausible.append(f"{s.sample.id}#{speech.get('speechIndex')}: "
                                   f"{chars} chars in {duration:.0f} s")
    metrics = {"checked": checked, "implausible": len(implausible)}
    if implausible:
        return Result(FAIL, metrics, _listing(implausible, 3))
    return Result(PASS, metrics)


def _samples_rights(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.published:
        return _unpublished()
    total = 0
    missing = Counter()
    for s in ctx.published:
        for speech in s.speeches:
            total += 1
            media = speech.get("media") or {}
            for key in ("creator", "license"):
                if not media.get(key):
                    missing[f"media.{key}"] += 1
            for tc in speech.get("textContents") or []:
                for key in ("creator", "license"):
                    if not tc.get(key):
                        missing[f"textContents.{key}"] += 1
    metrics = {"speeches": total, "missing": sum(missing.values())}
    if missing:
        return Result(FAIL, metrics, ", ".join(f"{name} x{count}" for name, count in sorted(missing.items())))
    return Result(PASS, metrics)


# ---- Whisper QC ----

def _no_whisper() -> Result:
    return Result(UNAVAILABLE, detail="no Whisper transcripts on this machine")


def _qc_whisper_coverage(ctx: Context) -> Result:
    if ctx.config is None:
        return _no_data()
    if not ctx.whisper_available:
        return _no_whisper()
    expected = sum(len(q.expected) for q in ctx.qc)
    if not expected:
        return Result(NOT_APPLICABLE, detail="the sample sessions carry no text")
    transcribed = sum(len(q.expected & q.transcribed) for q in ctx.qc)
    threshold = ctx.thresholds["whisper_coverage"]
    share = _share(transcribed, expected)
    metrics = {"share": share, "transcribed": transcribed, "expected": expected,
               "threshold": threshold}
    if share >= threshold:
        return Result(PASS, metrics)
    short = [f"{q.sample_id}: {len(q.expected & q.transcribed)} of {len(q.expected)}"
             + (f" ({q.outdated} transcripts outdated by a re-merge)" if q.outdated else "")
             for q in ctx.qc if q.expected and len(q.expected & q.transcribed) < len(q.expected)]
    return Result(FAIL, metrics, _listing(short))


def _similarities(ctx: Context) -> tuple[dict[str, list[float]], list[str], list[str], list[str]]:
    """Text similarity per sample session, over the speeches the platform would import with text. Also the sessions without QC figures, those whose figures are outdated, and those where no transcribed speech can be compared."""
    min_chars = ctx.thresholds["fidelity_min_chars"]
    by_sample = {s.sample.id: s for s in ctx.published}
    values: dict[str, list[float]] = {}
    missing, stale, empty = [], [], []
    for q in ctx.qc:
        if not q.expected:
            continue
        if q.rows is None:
            missing.append(q.sample_id)
            continue
        if not q.current:
            stale.append(q.sample_id)
            continue
        session_values = []
        for speech in by_sample[q.sample_id].speeches:
            index = speech.get("speechIndex")
            row = q.rows.get(index)
            # Figures of a transcript that no longer belongs to the speech say nothing.
            if row is None or row.get("text_similarity") is None or index not in q.transcribed:
                continue
            if text_chars(speech) < min_chars or not would_import_text(speech):
                continue
            session_values.append(row["text_similarity"])
        if session_values:
            values[q.sample_id] = session_values
        elif q.transcribed:
            empty.append(q.sample_id)
        else:
            missing.append(q.sample_id)
    return values, missing, stale, empty


def _qc_unsettled(ctx: Context) -> Result | None:
    """The result when the similarity figures cannot be judged yet, else ``None``."""
    if ctx.config is None:
        return _no_data()
    if not ctx.whisper_available:
        return _no_whisper()
    if not any(q.expected for q in ctx.qc):
        return Result(NOT_APPLICABLE, detail="the sample sessions carry no text")
    values, missing, stale, empty = _similarities(ctx)
    medians = {sid: round(statistics.median(v), 3) for sid, v in values.items() if v}
    if missing or empty:
        reasons = []
        if missing:
            reasons.append(f"no QC figures for {_listing(missing)}")
        if empty:
            reasons.append(f"no transcribed speech of {_listing(empty)} is long enough and imported with text")
        return Result(NOT_EVALUATED, {"median_similarity": medians} if medians else {}, "; ".join(reasons))
    if stale:
        return Result(STALE, {"median_similarity": medians} if medians else {},
                      f"text or transcript changed since the QC of {_listing(stale)}; run `qc {ctx.code}`")
    return None


def _qc_fidelity(ctx: Context) -> Result:
    unsettled = _qc_unsettled(ctx)
    if unsettled is not None:
        return unsettled
    values = _similarities(ctx)[0]
    pooled = [v for session_values in values.values() for v in session_values]
    medians = {sid: round(statistics.median(v), 3) for sid, v in values.items() if v}
    median = round(statistics.median(pooled), 3)
    threshold = ctx.thresholds["fidelity_median"]
    session_min = ctx.thresholds["fidelity_session_min"]
    metrics = {"median_similarity": median, "sessions": medians, "speeches": len(pooled),
               "threshold": threshold, "session_threshold": session_min}
    low = [f"{sid}: {value}" for sid, value in medians.items() if value < session_min]
    if median < threshold or low:
        return Result(FAIL, metrics, f"sessions below {session_min}: {_listing(low)}" if low else "")
    return Result(PASS, metrics)


def _qc_mismatch(ctx: Context) -> Result:
    unsettled = _qc_unsettled(ctx)
    if unsettled is not None:
        return Result(unsettled.status, detail=unsettled.detail)
    values = _similarities(ctx)[0]
    pooled = [v for session_values in values.values() for v in session_values]
    cutoff = ctx.thresholds["mismatch_similarity"]
    threshold = ctx.thresholds["mismatch_share"]
    mismatched = sum(1 for v in pooled if v < cutoff)
    share = _share(mismatched, len(pooled))
    metrics = {"share": share, "mismatched": mismatched, "speeches": len(pooled),
               "threshold": threshold}
    return Result(PASS if share <= threshold else FAIL, metrics)


def _qc_reviewed(ctx: Context) -> Result:
    attestation = ctx.declaration.attestations.get("qc_reviewed")
    if attestation is None:
        return Result(NOT_EVALUATED, detail="not attested")
    if ctx.config is None:
        return _no_data()
    if not ctx.whisper_available:
        return _no_whisper()
    if ctx.qc_evidence and attestation.get("evidence") == ctx.qc_evidence:
        return Result(PASS)
    return Result(STALE, detail=f"attested for evidence {attestation.get('evidence')!r}, "
                                f"the current evidence is {ctx.qc_evidence!r}")


def _attested(name: str) -> Callable[[Context], Result]:
    def check(ctx: Context) -> Result:
        if name in ctx.declaration.attestations:
            return Result(PASS)
        return Result(NOT_EVALUATED, detail="not attested")
    return check


CATALOGUE = [
    Criterion("impl.files", 2, "computed",
              "The parliament directory has manifest.yaml, workflow.py, common.py, README.md, scraper/, parsers/ and merger/.",
              _impl_files),
    Criterion("impl.manifest", 2, "computed",
              "The manifest resolves a locale, a language code, periods and known stages, and its readiness block is well-formed.",
              _impl_manifest),
    Criterion("impl.contract", 2, "computed",
              "common.py imports and exports a Config built on BaseConfig, and SessionStatus.",
              _impl_contract),
    Criterion("impl.tests", 2, "computed",
              "The parliament has a test module of its own under tests/.",
              _impl_tests),
    Criterion("samples.declared", 2, "computed",
              "At least three sample sessions are declared, and each one is published.",
              _samples_declared),
    Criterion("samples.stages", 2, "computed",
              "Every sample session went through all the stages the manifest supports.",
              _samples_stages),
    Criterion("samples.valid", 2, "computed",
              "The Stage 2 validator (full schema, semantic rules) finds no error and no sentence timed outside its media.",
              _samples_valid),
    Criterion("samples.import_fields", 2, "computed",
              "Every speech has the fields without which the platform importer rejects it.",
              _samples_import_fields),
    Criterion("samples.import_gate", 2, "computed",
              "No speech the merger marked as low confidence would be imported with its text.",
              _samples_import_gate),
    Criterion("samples.speaker_wid", 2, "computed",
              "The share of speeches whose first person has a Wikidata ID reaches the threshold.",
              _samples_speaker_wid),
    Criterion("samples.timing", 2, "computed",
              "The share of sentences with a start and end time reaches the threshold.",
              _samples_timing),
    Criterion("samples.text_plausible", 2, "computed",
              "No debate speech the merger passed carries more text than its clip could hold.",
              _samples_text_plausible),
    Criterion("samples.rights", 2, "computed",
              "Every media and text entry names its creator and licence.",
              _samples_rights),
    Criterion("qc.whisper_coverage", 3, "computed",
              "The share of text-bearing sample speeches with a Whisper transcript reaches the threshold.",
              _qc_whisper_coverage),
    Criterion("qc.fidelity", 3, "computed",
              "The median similarity between the published text and the transcript reaches the threshold, overall and in every session.",
              _qc_fidelity),
    Criterion("qc.mismatch", 3, "computed",
              "The share of speeches whose text does not match their audio stays under the threshold.",
              _qc_mismatch),
    Criterion("qc.reviewed", 3, "attested",
              "A person reviewed the Whisper reports of the current evidence.",
              _qc_reviewed),
    Criterion("release.licence", 3, "attested",
              "The rights to republish video and text are cleared.",
              _attested("licence_cleared")),
    Criterion("release.platform", 3, "attested",
              "The platform can import and present the data.",
              _attested("platform_ready")),
    Criterion("release.backfilled", 4, "attested",
              "The periods to be published are processed in full.",
              _attested("backfilled")),
    Criterion("release.live", 5, "attested",
              "A platform instance serves the data.",
              _attested("live")),
]
