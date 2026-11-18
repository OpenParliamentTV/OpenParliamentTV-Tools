"""The rebuild-queue pass of the shared runner.

A download step can find that a published session changed at the source in a way the publish guard refuses (DE-HH: a protocol replacing subtitle text; TW: official text arriving). It lists the session in a queue file, and ``run_workflow`` re-runs the queued sessions with rebuild semantics after the regular stages.
"""

import importlib
from types import SimpleNamespace

from optv.shared import workflow as wf
from optv.shared.config import BaseConfig

QUEUE = "upgrades.txt"


def _args(**overrides):
    values = dict(period=23, limit_to_period=True, limit_session="", force=False, rebuild=False,
                  download_original=True, merge_speeches=True, update_nel_entities=True,
                  link_entities=True, align_sentences=False, extract_entities=False)
    values.update(overrides)
    return SimpleNamespace(**values)


def _setup(tmp_path, monkeypatch, queued):
    config = BaseConfig(tmp_path)
    queue_file = config.dir("cache", create=True) / QUEUE
    if queued is not None:
        queue_file.write_text("".join(f"{s}\n" for s in queued))
    passes = []
    monkeypatch.setattr(wf, "_run_stages",
                        lambda config, args, hooks, state: passes.append((args, state)))
    return config, queue_file, passes


def test_no_queue_configured_runs_one_pass(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, ["23018"])
    args = _args()
    wf.run_workflow(config, args, wf.WorkflowHooks(parliament_id="DE"))
    assert [a for a, _ in passes] == [args]
    assert queue_file.read_text().split() == ["23018"]


def test_empty_or_missing_queue_runs_one_pass(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, None)
    wf.run_workflow(config, _args(), wf.WorkflowHooks(parliament_id="DE-HH", rebuild_queue=QUEUE))
    assert len(passes) == 1
    assert not queue_file.exists()


def test_queued_sessions_get_a_rebuild_pass(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, ["23018", "23019"])
    args = _args()
    wf.run_workflow(config, args, wf.WorkflowHooks(parliament_id="DE-HH", rebuild_queue=QUEUE))

    assert len(passes) == 2
    second, state = passes[1]
    assert second.rebuild is True and second.force is True
    assert second.download_original is False and second.update_nel_entities is False
    assert second.merge_speeches is True and second.link_entities is True
    assert wf._default_session_in_scope(second, "23018")
    assert wf._default_session_in_scope(second, "23019")
    assert not wf._default_session_in_scope(second, "23020")
    assert not wf._default_session_in_scope(second, "230180")
    # The narrowed pass records no watermarks and leaves the caller's args alone.
    assert state == {} and state is not passes[0][1]
    assert args.rebuild is False and args.download_original is True and args.limit_session == ""
    assert not queue_file.exists()


def test_out_of_scope_sessions_stay_queued(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, ["22101", "23018"])
    wf.run_workflow(config, _args(), wf.WorkflowHooks(parliament_id="DE-HH", rebuild_queue=QUEUE))
    assert len(passes) == 2
    assert wf._default_session_in_scope(passes[1][0], "23018")
    assert queue_file.read_text().split() == ["22101"]


def test_nothing_in_scope_leaves_the_queue_untouched(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, ["22101"])
    wf.run_workflow(config, _args(), wf.WorkflowHooks(parliament_id="DE-HH", rebuild_queue=QUEUE))
    assert len(passes) == 1
    assert queue_file.read_text().split() == ["22101"]


def test_run_without_merge_does_not_touch_the_queue(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, ["23018"])
    wf.run_workflow(config, _args(merge_speeches=False),
                    wf.WorkflowHooks(parliament_id="DE-HH", rebuild_queue=QUEUE))
    assert len(passes) == 1
    assert queue_file.read_text().split() == ["23018"]


def test_rebuild_run_only_clears_the_queue(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, ["23018"])
    wf.run_workflow(config, _args(rebuild=True, force=True),
                    wf.WorkflowHooks(parliament_id="DE-HH", rebuild_queue=QUEUE))
    assert len(passes) == 1
    assert not queue_file.exists()


def test_custom_scope_hook_decides_what_is_rebuilt(tmp_path, monkeypatch):
    config, queue_file, passes = _setup(tmp_path, monkeypatch, ["a", "b"])
    hooks = wf.WorkflowHooks(parliament_id="TW", rebuild_queue=QUEUE,
                             session_in_scope=lambda args, session: session == "b")
    wf.run_workflow(config, _args(), hooks)
    assert len(passes) == 2
    assert queue_file.read_text().split() == ["a"]


def test_de_hh_and_tw_name_the_queue_their_scrapers_write():
    hh = importlib.import_module("optv.parliaments.DE-HH.workflow")
    hh_scraper = importlib.import_module("optv.parliaments.DE-HH.scraper.fetch_proceedings")
    assert hh.HOOKS.rebuild_queue == hh_scraper.UPGRADES_FILE == "de-hh-protocol-upgrades.txt"
    tw = importlib.import_module("optv.parliaments.TW.workflow")
    tw_common = importlib.import_module("optv.parliaments.TW.common")
    assert tw.HOOKS.rebuild_queue == tw_common.OFFICIAL_TEXT_UPGRADES_FILE == "tw-official-text-upgrades.txt"


def test_de_has_no_rebuild_queue():
    de = importlib.import_module("optv.parliaments.DE.workflow")
    assert de.HOOKS.rebuild_queue is None
