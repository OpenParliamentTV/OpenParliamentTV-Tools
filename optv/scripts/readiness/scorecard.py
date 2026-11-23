"""One parliament's scorecard: every criterion's result and the level they add up to.

Levels 2 and 3 are computed: a parliament holds a level once every criterion gating it, and every level below, is satisfied. Levels 4 (backfilled) and 5 (live) are declared in the manifest. A parliament at a declared level keeps it when a lower criterion fails; the failure is listed as a regression.
"""

from __future__ import annotations

import datetime
import json
import os
import subprocess
from hashlib import blake2b
from pathlib import Path

from optv.parliaments import load_manifest
from optv.shared.publish import text_signature

from . import config as cfg
from .criteria import (CATALOGUE, FAIL, NOT_EVALUATED, SATISFIED, STALE, UNAVAILABLE, Context,
                       SampleData, SessionQC)
from .speeches import has_text, speech_duration

SCHEMA = 1
COMPUTED_LEVELS = (2, 3)


def scorecard_path(code: str, tools: Path | None = None) -> Path:
    return cfg.readiness_dir(tools) / f"{code}.json"


def load_scorecard(code: str, tools: Path | None = None) -> dict | None:
    path = scorecard_path(code, tools)
    if not path.is_file():
        return None
    return json.loads(path.read_text())


def _load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def _session_qc(data_dir: Path, sample: SampleData) -> SessionQC:
    from optv.shared.whisper_diff import current_transcripts, qc_path, whisper_path
    qc = SessionQC(sample.sample.id)
    if not sample.sample.expect_text:
        return qc
    qc.expected = {speech.get("speechIndex") for speech in sample.speeches if has_text(speech)}
    whisper = _load_json(whisper_path(data_dir, sample.sample.id))
    if whisper is not None:
        current, qc.outdated = current_transcripts(sample.speeches, whisper)
        qc.transcribed = set(current)
    figures = _load_json(qc_path(data_dir, sample.sample.id))
    if figures is not None and whisper is not None:
        qc.rows = {row.get("speechIndex"): row for row in figures.get("rows") or []}
        qc.current = (figures.get("text_signature") == text_signature(sample.speeches)
                      and figures.get("whisper_generated_at") == whisper.get("generated_at"))
        qc.evidence = (f"{qc.sample_id}:{figures.get('text_signature')}:"
                       f"{figures.get('whisper_generated_at')}:{len(qc.transcribed)}")
    return qc


def build_context(code: str, *, tools: Path | None = None, data_dir: Path | None = None,
                  config=None, declaration: cfg.Declaration | None = None) -> Context:
    tools = tools or cfg.tools_root()
    declaration = declaration or cfg.load_declaration(code)
    ctx = Context(code=code, tools=tools, manifest=load_manifest(code), declaration=declaration)
    if config is None:
        data_dir = data_dir or cfg.data_dir(code)
        if not data_dir.is_dir():
            return ctx
        try:
            from optv.shared.cli import load_config
            config = load_config(code, data_dir)
        except Exception:
            return ctx
    ctx.config = config
    data_dir = config.dir("data")
    for sample in declaration.samples:
        data = SampleData(sample)
        path = config.file(sample.id, "processed")
        if path.is_file():
            data.doc = _load_json(path)
            if data.doc is not None:
                data.status = config.status(sample.id)
        ctx.samples.append(data)
    ctx.whisper_available = (data_dir / "cache" / "whisper").is_dir()
    if ctx.whisper_available:
        ctx.qc = [_session_qc(data_dir, sample) for sample in ctx.published]
        evidence = sorted(q.evidence for q in ctx.qc if q.evidence)
        if evidence:
            ctx.qc_evidence = blake2b("\n".join(evidence).encode(), digest_size=8).hexdigest()
    return ctx


def _sample_summary(sample: SampleData) -> dict:
    seconds = 0.0
    guessed = False
    for speech in sample.speeches:
        duration, source = speech_duration(speech)
        seconds += duration
        guessed = guessed or source in ("text-length", "unknown")
    summary = {
        "speeches": len(sample.speeches),
        "text_speeches": sum(1 for speech in sample.speeches if has_text(speech)),
        "audio_seconds": round(seconds),
        "text": text_signature(sample.speeches)[:16],
    }
    if guessed:
        summary["audio_seconds_guessed"] = True
    if not sample.sample.expect_text:
        summary["expect_text"] = False
    return summary


def derive_level(criteria: dict) -> dict:
    """Level, what blocks the next one, and for a declared level what fails or is open below it."""
    def gates(level: int) -> list[str]:
        return [cid for cid, entry in criteria.items() if entry["level"] == level]

    def unsatisfied(level: int) -> list[str]:
        return [cid for cid in gates(level) if criteria[cid]["status"] not in SATISFIED]

    computed = 1
    for level in COMPUTED_LEVELS:
        if unsatisfied(level):
            break
        computed = level
    declared = max((level for level in (4, 5) if gates(level) and not unsatisfied(level)), default=0)
    level = max(computed, declared)

    blocking = unsatisfied(level + 1) if level < max(cfg.LEVELS) else []
    regressions, unverified = [], []
    if level > computed:
        for below in range(2, level + 1):
            for cid in unsatisfied(below):
                (regressions if criteria[cid]["status"] == FAIL else unverified).append(cid)
    return {"level": level, "level_name": cfg.LEVELS[level], "blocking": blocking,
            "regressions": regressions, "unverified": unverified}


def published_status(level: int) -> str:
    return cfg.STATUS_PRODUCTION if level == max(cfg.LEVELS) else cfg.STATUS_DEVELOPMENT


def evaluate(code: str, *, tools: Path | None = None, data_dir: Path | None = None, config=None,
             declaration: cfg.Declaration | None = None, previous: dict | None = None) -> dict:
    """Score one parliament. A criterion whose inputs are not on this machine keeps the result recorded in ``previous`` (by default the scorecard on disk)."""
    tools = tools or cfg.tools_root()
    ctx = build_context(code, tools=tools, data_dir=data_dir, config=config, declaration=declaration)
    if previous is None:
        previous = load_scorecard(code, tools) or {}

    criteria = {}
    for criterion in CATALOGUE:
        result = criterion.fn(ctx)
        recorded = (previous.get("criteria") or {}).get(criterion.id)
        if result.status == UNAVAILABLE and recorded:
            entry = dict(recorded, level=criterion.level)
        else:
            status = NOT_EVALUATED if result.status == UNAVAILABLE else result.status
            entry = {"level": criterion.level, "status": status}
            if result.metrics:
                entry["metrics"] = result.metrics
            if result.detail:
                entry["detail"] = result.detail
        criteria[criterion.id] = entry

    card = {"schema": SCHEMA, "parliament": code, "name": ctx.manifest.get("name", code)}
    card.update(derive_level(criteria))
    card["status"] = published_status(card["level"])
    if ctx.config is not None:
        card["samples"] = {s.sample.id: _sample_summary(s) for s in ctx.published}
    else:
        card["samples"] = previous.get("samples") or {}
    evidence = ctx.qc_evidence if ctx.whisper_available else previous.get("qc_evidence")
    if evidence:
        card["qc_evidence"] = evidence
    if ctx.declaration.overrides:
        card["thresholds"] = ctx.declaration.overrides
    card["criteria"] = criteria
    return card


def tools_revision(tools: Path) -> str:
    try:
        cp = subprocess.run(["git", "-C", str(tools), "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return cp.stdout.strip() if cp.returncode == 0 and cp.stdout.strip() else "unknown"


def _without_stamp(card: dict | None) -> dict:
    return {key: value for key, value in (card or {}).items() if key != "evaluated"}


def write_if_changed(card: dict, *, tools: Path | None = None, today: str | None = None,
                     revision: str | None = None) -> bool:
    """Write the scorecard unless nothing but its evaluation stamp would change. Returns whether it wrote."""
    tools = tools or cfg.tools_root()
    path = scorecard_path(card["parliament"], tools)
    previous = load_scorecard(card["parliament"], tools)
    if previous is not None and _without_stamp(previous) == _without_stamp(card):
        card["evaluated"] = previous.get("evaluated")
        return False
    card["evaluated"] = {"date": today or datetime.date.today().isoformat(),
                         "tools": revision or tools_revision(tools)}
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".json.partial")
    partial.write_text(json.dumps(card, indent=2, ensure_ascii=False) + "\n")
    os.replace(partial, path)
    return True
