"""Every module with a command line starts the way it is meant to be run: ``python -m <module>``.

The scraper, parser and merger modules of a parliament can be run on their own to exercise one part of a workflow. Nothing in the pipeline calls those entry points, so without this test a broken import or argument parser in one of them goes unnoticed.
"""

import re
import runpy
import sys
import warnings
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_MAIN = re.compile(r"^if __name__ == [\"']__main__[\"']:", re.M)


def _modules_with_a_command_line() -> list[str]:
    modules = []
    for path in sorted((ROOT / "optv").rglob("*.py")):
        if _MAIN.search(path.read_text()):
            modules.append(".".join(path.relative_to(ROOT).with_suffix("").parts))
    return modules


MODULES = _modules_with_a_command_line()


def test_there_are_command_lines_to_check():
    assert len(MODULES) > 100
    assert "optv.parliaments.DE.workflow" in MODULES
    assert "optv.parliaments.DE-BW.merger.merge_session" in MODULES


@pytest.mark.parametrize("module", MODULES)
def test_module_prints_its_usage(module, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", [module, "--help"])
    with warnings.catch_warnings():
        # runpy warns when the module is already imported; running it again as __main__ is the point.
        warnings.simplefilter("ignore", RuntimeWarning)
        with pytest.raises(SystemExit) as exit_info:
            runpy.run_module(module, run_name="__main__")
    assert exit_info.value.code == 0
    assert "usage" in capsys.readouterr().out.lower()
