"""The surface the Conductor depends on, for every parliament.

The Conductor runs ``python -m optv.parliaments.<id>.workflow <argv>`` as a subprocess, imports ``optv.parliaments.<id>.common`` in-process for session listing and status, reads the manifest, and advances job progress by matching workflow log lines. Its own test suite fakes the Tools side, so this file is where a change that breaks that surface fails.
"""

import importlib
import json
import logging
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from optv.parliaments import list_parliaments, load_manifest
from optv.shared import workflow as shared_workflow
from optv.shared.config import BaseConfig

PARLIAMENTS = list_parliaments()

# Mirrors of the Conductor's src/workflow/progress.py patterns.
SESSION_PATTERNS = {
    "publish": re.compile(r"Publishing (\d+) from"),
    "align": re.compile(r"Time-aligning (\d+)"),
    "nel": re.compile(r"Linking entities for (\d+) from"),
    "ner": re.compile(r"Extracting Named Entities for (\d+)"),
}
STAGE_PATTERNS = {
    "download": re.compile(r"Downloading media and proceeding data"),
    "merge": re.compile(r"Merging data from"),
    "nel": re.compile(r"Linking entities with wikidata"),
    "align": re.compile(r"Updating time-alignment"),
    "ner": re.compile(r"Updating NER"),
}


def _conductor_argv(parliament: str, data_dir: Path, *, session_filter: str = "",
                    stages=("download", "merge", "nel", "align", "ner"),
                    force=False, rebuild=False, entity_url: str = "") -> list[str]:
    """The argument list the Conductor's runner builds for one job."""
    manifest = load_manifest(parliament)
    argv = [
        str(data_dir),
        f"--period={max(manifest['periods'])}",
        f"--retry-count={manifest['default_retry_count']}",
        f"--cache-dir={data_dir / 'cache'}",
        f"--lang={manifest['language']}",
        f"--limit-session={session_filter}",
        "--ner-api-endpoint=http://localhost:8090/service",
        "--align-timeout=1200",
        "--align-max-audio-seconds=2400",
        "--no-single-instance",
        "--no-limit-to-period" if session_filter else "--limit-to-period",
    ]
    if force:
        argv.append("--force")
    if rebuild:
        argv.append("--rebuild")
    if "download" in stages:
        argv.append("--download-original")
    if "merge" in stages:
        argv.append("--merge-speeches")
    if "align" in stages:
        argv.append("--align-sentences")
    argv.append("--update-nel-entities")
    if entity_url:
        argv.append(f"--nel-entity-url={entity_url}")
    if "nel" in stages:
        argv.append("--link-entities")
    if "ner" in stages:
        argv.append("--extract-entities")
    return argv


# ---- in-process: optv.parliaments.<id>.common ----

@pytest.mark.parametrize("parliament", PARLIAMENTS)
def test_common_exposes_config_and_session_status(parliament, tmp_path):
    common = importlib.import_module(f"optv.parliaments.{parliament}.common")
    config = common.Config(tmp_path)
    assert isinstance(config, BaseConfig)

    assert config.sessions() == []
    assert config.sessions(prefix="21") == []
    assert config.status("00000") == set()
    assert isinstance(config.is_newer("00000", "merged", "aligned"), bool)
    assert config.file("00000", "processed") == tmp_path / "processed" / "00000-session.json"
    assert tmp_path in config.file("00000", "media").parents

    for flag in ("media", "proceedings", "merged", "session", "empty",
                 "linked", "no_text", "aligned", "ner"):
        assert hasattr(common.SessionStatus, flag)


@pytest.mark.parametrize("parliament", PARLIAMENTS)
def test_manifest_carries_the_keys_the_conductor_maps(parliament):
    manifest = load_manifest(parliament)
    for key in ("name", "language", "periods", "supported_stages",
                "entity_dump_url", "default_retry_count"):
        assert key in manifest, f"{parliament} manifest lacks {key}"
    assert manifest["periods"] and all(isinstance(p, int) for p in manifest["periods"])
    assert set(manifest["supported_stages"]) <= {"download", "parse", "merge", "nel", "align", "ner"}


# ---- subprocess entry point: python -m optv.parliaments.<id>.workflow ----

@pytest.mark.parametrize("parliament", PARLIAMENTS)
@pytest.mark.parametrize("job", ["scheduled", "rerun"])
def test_workflow_accepts_the_conductor_argv(parliament, job, tmp_path, monkeypatch):
    """Every wrapper parses the Conductor's argv and reaches the shared runner with a usable config, args and hook set."""
    captured = {}
    monkeypatch.setattr(shared_workflow, "run_workflow",
                        lambda config, args, hooks: captured.update(config=config, args=args, hooks=hooks))
    if job == "scheduled":
        argv = _conductor_argv(parliament, tmp_path)
    else:
        argv = _conductor_argv(parliament, tmp_path, session_filter="^(21001|21002)$",
                               stages=("merge", "nel"), force=True, rebuild=True,
                               entity_url="https://example.invalid/entities.json")
    monkeypatch.setattr(sys, "argv", ["workflow", *argv])

    workflow = importlib.import_module(f"optv.parliaments.{parliament}.workflow")
    workflow.main()

    common = importlib.import_module(f"optv.parliaments.{parliament}.common")
    assert isinstance(captured["config"], common.Config)
    assert captured["config"].dir("merged") == tmp_path / "cache" / "merged"
    hooks, args = captured["hooks"], captured["args"]
    assert hooks.parliament_id == parliament
    assert callable(hooks.merge_session_to_file)
    assert args.period == max(load_manifest(parliament)["periods"])
    assert args.single_instance is False
    assert args.merge_speeches and args.link_entities and args.update_nel_entities
    assert args.rebuild is (job == "rerun")
    assert args.force is (job == "rerun")
    assert args.limit_to_period is (job == "scheduled")
    for name in ("spacy_model", "aeneas_language", "entityfishing_language"):
        assert getattr(args, name)


def test_workflow_modules_exist_for_every_manifest():
    for parliament in PARLIAMENTS:
        package = Path(importlib.import_module(f"optv.parliaments.{parliament}").__file__).parent
        assert (package / "workflow.py").is_file()
        assert (package / "common.py").is_file()


# ---- log lines the Conductor matches for stage and session progress ----

def _stage_args(data_dir, **overrides):
    values = dict(force=False, rebuild=False, limit_to_period=True, period=21,
                  limit_session="", data_dir=data_dir, validate=False,
                  ner_corrections=False, ner_api_endpoint="")
    values.update(overrides)
    return SimpleNamespace(**values)


def _write(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc), encoding="utf-8")


def _speech(**debug):
    return {"people": [{"label": "A"}], "debug": {"proceedingIndex": 1, **debug},
            "textContents": [{"textBody": [{"sentences": [{"text": "x"}]}]}]}


def _messages(caplog):
    return [record.getMessage() for record in caplog.records]


def test_stage_runners_emit_the_progress_lines(tmp_path, caplog, monkeypatch):
    config = BaseConfig(tmp_path)
    in_scope = shared_workflow._default_session_in_scope
    published = []
    publish = lambda session, path: published.append((session, path.name))  # noqa: E731
    _write(config.file("21001", "media"), {"data": []})

    class Hooks:
        parliament_id = "DE"

        def merge_session_to_file(self, cfg, session, args):
            _write(cfg.file(session, "merged"), {"meta": {}, "data": [_speech()]})
            return cfg.file(session, "merged")

        def align_session_to_file(self, cfg, session, args):
            _write(cfg.file(session, "aligned"), {"meta": {}, "data": [_speech(alignDuration=1.0)]})
            return cfg.file(session, "aligned")

    def fake_link(source, target, persons, factions, clear_existing=False):
        return None

    def fake_extract(source, target, args, corrections):
        _write(target, {"meta": {}, "data": [_speech(alignDuration=1.0, nerDuration=1.0)]})

    monkeypatch.setattr(shared_workflow, "link_entities_from_file", fake_link)
    monkeypatch.setattr(shared_workflow, "extract_entities_from_file", fake_extract)
    monkeypatch.setattr(shared_workflow, "get_nel_data", lambda directory: ({}, {}))
    _write(config.dir("nel_data") / "entities.json", {"meta": {}, "data": []})

    args = _stage_args(tmp_path)
    with caplog.at_level(logging.INFO):
        shared_workflow._run_merge_stage(config, args, Hooks(), in_scope, publish, {}, "21")
        _write(config.file("21001", "processed"), {"meta": {"processing": {"merge": "2026-01-01T00:00:00"}},
                                                    "data": [_speech()]})
        shared_workflow._run_nel_stage(config, args, in_scope, publish, {}, "21")
        shared_workflow._run_align_stage(config, args, Hooks(), in_scope, publish, {}, "21")
        shared_workflow._run_ner_stage(config, args, in_scope, publish, {}, "21")

    messages = _messages(caplog)
    for stage in ("merge", "nel", "align", "ner"):
        assert any(STAGE_PATTERNS[stage].search(m) for m in messages), f"no {stage} stage banner"
    for stage in ("nel", "align", "ner"):
        matches = [SESSION_PATTERNS[stage].search(m) for m in messages]
        assert any(m and m.group(1) == "21001" for m in matches), f"no {stage} session line"
    assert [session for session, _ in published] == ["21001"] * 4


def test_publish_emits_the_session_progress_line(tmp_path, caplog):
    config = BaseConfig(tmp_path)
    doc = {"meta": {"session": "21001", "processing": {"merge": "2026-01-01T00:00:00"}},
           "data": [_speech()]}
    _write(config.file("21001", "merged"), doc)
    with caplog.at_level(logging.INFO):
        shared_workflow._publish_as_processed(config, _stage_args(tmp_path), "21001",
                                              config.file("21001", "merged"))
    matches = [SESSION_PATTERNS["publish"].search(m) for m in _messages(caplog)]
    assert any(m and m.group(1) == "21001" for m in matches)
    assert config.file("21001", "processed").exists()


def test_de_download_emits_the_stage_banner(tmp_path, caplog, monkeypatch):
    workflow = importlib.import_module("optv.parliaments.DE.workflow")
    monkeypatch.setattr(workflow, "update_media_directory_period", lambda *a, **k: None)
    monkeypatch.setattr(workflow, "download_plenary_protocols", lambda *a, **k: None)
    common = importlib.import_module("optv.parliaments.DE.common")
    args = SimpleNamespace(period=21, force=False, retry_count=0)
    with caplog.at_level(logging.INFO):
        workflow._download(common.Config(tmp_path), args)
    assert any(STAGE_PATTERNS["download"].search(m) for m in _messages(caplog))
