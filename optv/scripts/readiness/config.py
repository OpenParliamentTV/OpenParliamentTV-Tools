"""Paths, declared facts and default thresholds of the readiness tooling.

The ``readiness`` block of a ``manifest.yaml``::

    readiness:
      sample_sessions:            # keys as Config.sessions() returns them, quoted
        - "19078"
        - {id: "18009", expect_text: false, note: "WP 18: video without subtitles"}
      attestations:               # what a person has to state; absent = not attested
        qc_reviewed: {evidence: "<qc_evidence of the scorecard>", note: "..."}
        licence_cleared: {note: "..."}
        platform_ready: {note: "..."}
        backfilled: {periods: [19]}
        live: {url: "https://..."}
      thresholds:                 # per-parliament override of a default, with its reason
        speaker_wid_share: {value: 0.90, reason: "..."}
      whisper: {language: "no"}   # Whisper language, when the speeches' own code does not fit
      sample_run:                 # how to process the sample sessions and nothing else
        - sessions: ["19078", "19079", "19080"]
          args: ["--period=19", "--limit-session", "^1907[89]$|^19080$"]

``sample_run`` lists workflow invocations: the sample sessions each one covers and the arguments that confine it to them, downloads included. Whoever declares one has read the parliament's download hook and knows the arguments hold there. Without it the tooling never runs the workflow. The stages are added by the tooling; an entry names none, and neither ``--force`` nor ``--rebuild``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from optv.parliaments import load_manifest

DATA_REPO_PREFIX = "OpenParliamentTV-Data-"

LEVELS = {1: "implemented", 2: "samples pass", 3: "QC cleared", 4: "backfilled", 5: "live"}
STATUS_PRODUCTION = "production"
STATUS_DEVELOPMENT = "in development"

ATTESTATIONS = ("qc_reviewed", "licence_cleared", "platform_ready", "backfilled", "live")

# Workflow arguments that confine a run to named sessions. A sample run carries one of them.
SCOPE_FLAGS = ("--limit-session", "--session", "--sitting", "--kid")
# Arguments a sample run never takes from a manifest: the tooling sets the stages, and a
# forced or rebuilding run is a deliberate act by hand.
RESERVED_FLAGS = ("--download-original", "--merge-speeches", "--link-entities", "--align-sentences",
                  "--extract-entities", "--update-nel-entities", "--force", "--rebuild",
                  "--single-instance", "--no-single-instance")

DEFAULT_THRESHOLDS = {
    # Fewest sample sessions a parliament declares.
    "min_samples": 3,
    # Share of speeches whose first person carries a Wikidata ID.
    "speaker_wid_share": 0.95,
    # Share of sentences with a start and end time.
    "timing_share": 0.95,
    # A speech this long whose text is this dense for its clip carries the wrong text.
    "implausible_min_chars": 800,
    "implausible_cps": 100,
    # Share of the text-bearing sample speeches that have a Whisper transcript.
    "whisper_coverage": 0.95,
    # Median text similarity over all samples, and the lowest median of one session.
    "fidelity_median": 0.80,
    "fidelity_session_min": 0.70,
    # Speeches shorter than this are left out of the similarity figures.
    "fidelity_min_chars": 200,
    # A speech below this similarity counts as a mismatch; highest share of them.
    "mismatch_similarity": 0.5,
    "mismatch_share": 0.05,
}


def tools_root() -> Path:
    return Path(__file__).resolve().parents[3]


def data_root() -> Path:
    """Directory holding the ``OpenParliamentTV-Data-<CODE>`` checkouts."""
    override = os.environ.get("OPTV_DATA_ROOT")
    return Path(override) if override else tools_root().parent


def data_dir(code: str) -> Path:
    return data_root() / f"{DATA_REPO_PREFIX}{code}"


def readiness_dir(tools: Path | None = None) -> Path:
    return (tools or tools_root()) / "docs" / "readiness"


def overview_path(tools: Path | None = None) -> Path:
    return (tools or tools_root()) / "docs" / "READINESS.md"


def state_dir() -> Path:
    """Job plans, state and logs. Outside every repository, so a job survives a ``git clean`` and nothing of it can be committed or published."""
    override = os.environ.get("OPTV_STATE_DIR")
    return Path(override) if override else Path.home() / ".local" / "state" / "optv-readiness"


@dataclass(frozen=True)
class Sample:
    id: str
    expect_text: bool = True
    note: str = ""


@dataclass(frozen=True)
class SampleRun:
    sessions: tuple
    args: tuple


@dataclass
class Declaration:
    samples: list[Sample] = field(default_factory=list)
    sample_runs: list[SampleRun] = field(default_factory=list)
    attestations: dict = field(default_factory=dict)
    thresholds: dict = field(default_factory=lambda: dict(DEFAULT_THRESHOLDS))
    overrides: dict = field(default_factory=dict)
    whisper_language: str | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def sample_ids(self) -> list[str]:
        return [s.id for s in self.samples]

    @property
    def is_live(self) -> bool:
        return "live" in self.attestations


def parse_declaration(block) -> Declaration:
    """Read a ``readiness`` block. Problems are collected in ``errors``; what can be read is kept."""
    decl = Declaration()
    if block is None:
        decl.errors.append("no readiness block")
        return decl
    if not isinstance(block, dict):
        decl.errors.append("readiness is not a mapping")
        return decl
    unknown = sorted(set(block) - {"sample_sessions", "attestations", "thresholds", "whisper",
                                   "sample_run"})
    if unknown:
        decl.errors.append(f"unknown keys: {', '.join(unknown)}")

    entries = block.get("sample_sessions")
    if not isinstance(entries, list):
        decl.errors.append("sample_sessions is not a list")
        entries = []
    seen = set()
    for entry in entries:
        if isinstance(entry, dict):
            sid, expect_text, note = entry.get("id"), entry.get("expect_text", True), entry.get("note", "")
            extra = sorted(set(entry) - {"id", "expect_text", "note"})
            if extra:
                decl.errors.append(f"sample {sid!r}: unknown keys {', '.join(extra)}")
        else:
            sid, expect_text, note = entry, True, ""
        if not isinstance(sid, str) or not sid:
            decl.errors.append(f"sample {sid!r}: the session key must be a quoted string")
            continue
        if not isinstance(expect_text, bool) or not isinstance(note, str):
            decl.errors.append(f"sample {sid!r}: expect_text is a boolean, note a string")
            continue
        if sid in seen:
            decl.errors.append(f"sample {sid!r} is listed twice")
            continue
        seen.add(sid)
        decl.samples.append(Sample(sid, expect_text, note))

    attestations = block.get("attestations") or {}
    if not isinstance(attestations, dict):
        decl.errors.append("attestations is not a mapping")
        attestations = {}
    for name, value in attestations.items():
        if name not in ATTESTATIONS:
            decl.errors.append(f"unknown attestation {name!r}")
        elif not isinstance(value, dict):
            decl.errors.append(f"attestation {name!r} is not a mapping")
        else:
            decl.attestations[name] = value

    thresholds = block.get("thresholds") or {}
    if not isinstance(thresholds, dict):
        decl.errors.append("thresholds is not a mapping")
        thresholds = {}
    for name, value in thresholds.items():
        if name not in DEFAULT_THRESHOLDS:
            decl.errors.append(f"unknown threshold {name!r}")
        elif (not isinstance(value, dict) or isinstance(value.get("value"), bool)
              or not isinstance(value.get("value"), (int, float)) or not value.get("reason")):
            decl.errors.append(f"threshold {name!r} needs a numeric value and a reason")
        else:
            decl.thresholds[name] = value["value"]
            decl.overrides[name] = {"value": value["value"], "reason": value["reason"]}

    runs = block.get("sample_run") or []
    if not isinstance(runs, list):
        decl.errors.append("sample_run is not a list")
        runs = []
    for number, entry in enumerate(runs, start=1):
        problem = _sample_run_problem(entry, seen)
        if problem:
            decl.errors.append(f"sample_run {number}: {problem}")
        else:
            decl.sample_runs.append(SampleRun(tuple(entry["sessions"]), tuple(entry["args"])))

    whisper = block.get("whisper") or {}
    if not isinstance(whisper, dict) or set(whisper) - {"language"}:
        decl.errors.append("whisper takes only a language")
    elif whisper.get("language") is not None:
        if isinstance(whisper["language"], str) and whisper["language"]:
            decl.whisper_language = whisper["language"]
        else:
            decl.errors.append("whisper.language is not a string")
    return decl


def _flag(argument: str) -> str:
    return argument.split("=", 1)[0]


def _sample_run_problem(entry, samples: set) -> str | None:
    """What is wrong with one ``sample_run`` entry, if anything."""
    if not isinstance(entry, dict) or set(entry) != {"sessions", "args"}:
        return "takes sessions and args"
    sessions, args = entry["sessions"], entry["args"]
    if not (isinstance(sessions, list) and sessions and all(isinstance(s, str) for s in sessions)):
        return "sessions is a list of quoted session keys"
    if not (isinstance(args, list) and args and all(isinstance(a, str) for a in args)):
        return "args is a list of strings"
    outside = [s for s in sessions if s not in samples]
    if outside:
        return f"{', '.join(outside)} is not a sample session"
    if not any(_flag(a) in SCOPE_FLAGS for a in args):
        return f"no argument confines the run to sessions ({', '.join(SCOPE_FLAGS)})"
    reserved = [a for a in args if _flag(a) in RESERVED_FLAGS]
    if reserved:
        return f"{', '.join(reserved)} is not for a manifest to set"
    return None


def load_declaration(code: str) -> Declaration:
    return parse_declaration(load_manifest(code).get("readiness"))
