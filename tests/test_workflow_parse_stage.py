"""Parse stage: when it runs, and how it degrades without ParlaMint registries.

Both behaviours are regressions we actually hit: an NEL-only run used to re-parse
the originals and die on a session it was never going to touch (the registries are
not versioned in the data repo, and neither is the parsed JSON they produce).
"""

import argparse
import json
import os

import pytest

from optv.parliaments.DE.common import Config
from optv.parliaments.DE.parsers.parlamint2json import (REGISTRY_FILES,
                                                        parse_parlamint_directory)
from optv.shared.workflow import WorkflowHooks, run_workflow


def _args(**kw):
    base = dict(period=17, force=False, limit_session=None, no_limit_to_period=False,
                download_original=False, merge_speeches=False, update_nel_entities=False,
                link_entities=False, align_sentences=False, extract_entities=False,
                rebuild=False, validate=False, debug=False)
    base.update(kw)
    return argparse.Namespace(**base)


@pytest.mark.parametrize("flag,should_parse", [
    ("link_entities", False),
    ("align_sentences", False),
    ("extract_entities", False),
    ("merge_speeches", True),
    ("download_original", True),
])
def test_parse_runs_only_for_download_and_merge(tmp_path, flag, should_parse):
    """Parsing produces merge's inputs, so runs starting below merge must skip it."""
    called = []
    hooks = WorkflowHooks(parliament_id='DE',
                          parse_originals=lambda config, args: called.append(True),
                          download_originals=lambda config, args: None)
    run_workflow(Config(tmp_path), _args(**{flag: True}), hooks)
    assert bool(called) is should_parse


def _session_xml(sid):
    # Minimal ParlaMint-shaped stub: parse_parlamint_directory only sniffs for
    # "parla.sitting" before committing to a parse.
    return f'<TEI><teiHeader><title>parla.sitting {sid}</title></teiHeader></TEI>'


def test_missing_registries_keep_existing_parse_and_do_not_raise(tmp_path, caplog):
    """A stale mtime on an already-parsed session must not abort the whole run.

    `-data.xml` is versioned and `-proceedings.json` is not, so a checkout can
    leave the XML looking newer than its parse. Without the registries we cannot
    redo it -- keep what we have rather than taking the run down.
    """
    (tmp_path / "17053-data.xml").write_text(_session_xml("17053"))
    parsed = tmp_path / "17053-proceedings.json"
    parsed.write_text(json.dumps([{"kept": True}]))
    os.utime(parsed, (0, 0))  # epoch: older than the XML -> looks stale

    assert not any((tmp_path / name).exists() for name in REGISTRY_FILES)

    parse_parlamint_directory(tmp_path, _args())  # must not raise

    assert json.loads(parsed.read_text()) == [{"kept": True}]
    assert "keeping existing" in caplog.text


def test_missing_registries_report_unparseable_sessions(tmp_path, caplog):
    """A session with no parse at all cannot be recovered -- say so, don't crash."""
    (tmp_path / "17169-data.xml").write_text(_session_xml("17169"))

    parse_parlamint_directory(tmp_path, _args())  # must not raise

    assert not (tmp_path / "17169-proceedings.json").exists()
    assert "left unparsed" in caplog.text
    assert "17169-data.xml" in caplog.text
