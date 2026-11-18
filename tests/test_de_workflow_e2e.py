"""DE end to end through the real workflow entry point.

``test_de_golden`` calls the parser and merger functions directly. This test drives ``optv.parliaments.DE.workflow.main()`` the way the Conductor does (argv in, files out), so the wrapper, the shared argparser and runner, ``Config``, the directory parsers and the publish step are all on the path. The merged file must carry exactly the speeches of the committed golden master.
"""

import importlib
import json
import logging
import re
import shutil
import sys
from pathlib import Path

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "DE"
GOLDEN = FIXTURES / "golden" / "tiny-session.json"
SESSION = "21999"


def _seed(data_dir: Path) -> Path:
    media_dir = data_dir / "original" / "media"
    proceedings_dir = data_dir / "original" / "proceedings"
    media_dir.mkdir(parents=True)
    proceedings_dir.mkdir(parents=True)
    shutil.copy(FIXTURES / "tiny-media-input.json", media_dir / f"raw-{SESSION}-media.json")
    proceedings_file = proceedings_dir / f"{SESSION}-proceedings.xml"
    shutil.copy(FIXTURES / "tiny-proceedings.xml", proceedings_file)
    return proceedings_file


def _run_workflow(monkeypatch, data_dir: Path, *flags: str) -> None:
    workflow = importlib.import_module("optv.parliaments.DE.workflow")
    monkeypatch.setattr(sys, "argv", [
        "workflow", str(data_dir), "--period=21", "--retry-count=20",
        f"--cache-dir={data_dir / 'cache'}", "--lang=deu", "--limit-session=",
        "--no-single-instance", "--limit-to-period", *flags,
    ])
    workflow.main()


def test_de_workflow_merges_and_publishes_the_golden_speeches(tmp_path, monkeypatch, caplog):
    data_dir = tmp_path / "data"
    proceedings_file = _seed(data_dir)

    with caplog.at_level(logging.INFO):
        _run_workflow(monkeypatch, data_dir, "--merge-speeches")

    merged_file = data_dir / "cache" / "merged" / f"{SESSION}-merged.json"
    merged_text = merged_file.read_text()
    merged = json.loads(merged_text)
    assert merged_text == json.dumps(merged, indent=2, ensure_ascii=False)

    golden = json.loads(GOLDEN.read_text())
    rendered = json.dumps(merged["data"], indent=2, ensure_ascii=False)
    rendered = rendered.replace(str(proceedings_file), "<FIXTURES>/tiny-proceedings.xml")
    assert rendered == json.dumps(golden["data"], indent=2, ensure_ascii=False)
    for key in ("schemaVersion", "session", "dateStart", "dateEnd", "lastProcessing"):
        assert merged["meta"][key] == golden["meta"][key]

    published = json.loads((data_dir / "processed" / f"{SESSION}-session.json").read_text())
    assert len(published["data"]) == len(golden["data"])

    # The Conductor advances its stage and per-session progress on these lines.
    messages = [record.getMessage() for record in caplog.records]
    assert any(re.search(r"Merging data from", m) for m in messages)
    assert any(re.search(r"Publishing (\d+) from", m) for m in messages)


def test_de_workflow_second_run_is_a_noop(tmp_path, monkeypatch, caplog):
    data_dir = tmp_path / "data"
    _seed(data_dir)
    _run_workflow(monkeypatch, data_dir, "--merge-speeches")
    merged_file = data_dir / "cache" / "merged" / f"{SESSION}-merged.json"
    first = merged_file.read_text()

    caplog.clear()
    with caplog.at_level(logging.INFO):
        _run_workflow(monkeypatch, data_dir, "--merge-speeches")

    assert merged_file.read_text() == first
    messages = [record.getMessage() for record in caplog.records]
    assert any("nothing to merge" in m for m in messages)
    assert not any("Merging data from" in m for m in messages)
