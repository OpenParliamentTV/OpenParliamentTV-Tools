"""Work-aware stage logging + machine-local watermark/marker gating.

Locks in the behaviour that a no-op run logs one honest line per stage (never a
header that implies processing), that the per-period mtime watermark lets an
unchanged tree skip the scan, that a changed entity dump re-links the in-scope
corpus (Part B), and that the entity-dump fetch rejects a transient glitch while
treating an absent platform as a calm supported mode.
"""

import json
import logging
from types import SimpleNamespace

from optv.shared.config import BaseConfig
from optv.shared import workflow as wf

IN_SCOPE = wf._default_session_in_scope


def _args(**kw):
    base = dict(force=False, limit_to_period=True, period=21,
                limit_session=None, data_dir=None, nel_entity_url="",
                validate=False)
    base.update(kw)
    return SimpleNamespace(**base)


def _cfg(tmp_path):
    cfg = BaseConfig(tmp_path)
    for stage in ('media', 'processed', 'nel_data'):
        cfg.dir(stage, create=True)
    return cfg


def _media(cfg, session):
    cfg.file(session, 'media').write_text('{"data": []}', encoding='utf-8')


def _processed(cfg, session, processing):
    cfg.file(session, 'processed', create=True).write_text(
        json.dumps({"meta": {"processing": processing}, "data": []}),
        encoding='utf-8')


def _entities(cfg, data):
    (cfg.dir('nel_data') / 'entities.json').write_text(
        json.dumps({"meta": {}, "data": data}), encoding='utf-8')


class _FakeResp:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _patch_urlopen(monkeypatch, data):
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda url, timeout=120: _FakeResp(data))


# ---- canonical entity-dump version (Part B trigger) ----

def test_canonical_sha_ignores_serialization_but_not_content(tmp_path):
    cfg = _cfg(tmp_path)
    ent = [
        {"id": "Q1", "label": "B", "labelAlternative": ["x"], "subType": "person"},
        {"id": "Q2", "label": "A", "labelAlternative": [], "subType": "faction"},
    ]
    _entities(cfg, ent)
    sha = wf._entities_canonical_sha(cfg)
    # Reversed order + different whitespace + extra meta: same *content*.
    (cfg.dir('nel_data') / 'entities.json').write_text(
        json.dumps({"data": list(reversed(ent)), "meta": {"x": 1}}, indent=4),
        encoding='utf-8')
    assert wf._entities_canonical_sha(cfg) == sha
    # A real content change flips it.
    ent[0]["label"] = "B2"
    _entities(cfg, ent)
    assert wf._entities_canonical_sha(cfg) != sha


def test_canonical_sha_none_when_absent(tmp_path):
    assert wf._entities_canonical_sha(_cfg(tmp_path)) is None


# ---- merge stage: honest header vs no-op line ----

def test_merge_logs_header_then_noop(tmp_path, caplog):
    cfg = _cfg(tmp_path)
    _media(cfg, "21001")
    args = _args(data_dir=tmp_path)
    state = {}
    merged = []

    class Hooks:
        def merge_session_to_file(self, c, s, a):
            p = c.file(s, 'merged', create=True)
            p.write_text('{"data": []}', encoding='utf-8')
            merged.append(s)
            return p

    hooks = Hooks()
    with caplog.at_level(logging.INFO):
        wf._run_merge_stage(cfg, args, hooks, IN_SCOPE, lambda s, f: None, state, "21")
    assert merged == ["21001"]
    assert any("Merging data from" in r.message and "1 session" in r.message
               for r in caplog.records)

    merged.clear()
    caplog.clear()
    with caplog.at_level(logging.INFO):
        wf._run_merge_stage(cfg, args, hooks, IN_SCOPE, lambda s, f: None, state, "21")
    assert merged == []
    assert any("nothing to merge" in r.message for r in caplog.records)
    assert not any("Merging data from" in r.message for r in caplog.records)
    # an all-clear pass records the merge watermark for this period
    assert "merge" in state["21"]


# ---- nel stage: work, settle, then Part B relink ----

def test_nel_links_then_settles(tmp_path, caplog):
    cfg = _cfg(tmp_path)
    _media(cfg, "21001")
    _entities(cfg, [])
    _processed(cfg, "21001", {"merge": "2026-06-18T10:00:00"})  # no nel pass yet
    args = _args(data_dir=tmp_path)
    state = {}
    pub = []
    with caplog.at_level(logging.INFO):
        wf._run_nel_stage(cfg, args, IN_SCOPE, lambda s, f: pub.append(s), state, "21")
    assert pub == ["21001"]
    assert any("Linking entities with wikidata IDs" in r.message for r in caplog.records)
    assert state["21"]["nel_entities_sha"] == wf._entities_canonical_sha(cfg)

    # link_entities_from_file stamped meta.processing.nel into the processed
    # file, so the next pass finds nothing and says so honestly.
    caplog.clear()
    with caplog.at_level(logging.INFO):
        wf._run_nel_stage(cfg, args, IN_SCOPE, lambda s, f: pub.append(s), state, "21")
    assert pub == ["21001"]
    assert any("nothing to link" in r.message for r in caplog.records)
    assert not any("Linking entities with wikidata IDs" in r.message
                   for r in caplog.records)


def test_nel_relinks_when_dump_changes(tmp_path, caplog):
    cfg = _cfg(tmp_path)
    _media(cfg, "21001")
    _entities(cfg, [])
    # Already linked and current (nel >= merge) AND a watermark that would
    # otherwise skip the scan -- only a dump-version change must override both.
    _processed(cfg, "21001",
               {"merge": "2026-06-18T10:00:00", "nel": "2026-06-18T11:00:00"})
    args = _args(data_dir=tmp_path)
    state = {"21": {"nel_entities_sha": "stale-sha", "nel": 9e18}}
    pub = []
    with caplog.at_level(logging.INFO):
        wf._run_nel_stage(cfg, args, IN_SCOPE, lambda s, f: pub.append(s), state, "21")
    assert pub == ["21001"]
    assert any("Linking entities with wikidata IDs" in r.message for r in caplog.records)
    assert state["21"]["nel_entities_sha"] == wf._entities_canonical_sha(cfg)


def test_nel_no_entities_file_is_honest(tmp_path, caplog):
    cfg = _cfg(tmp_path)
    _media(cfg, "21001")
    _processed(cfg, "21001", {"merge": "2026-06-18T10:00:00"})
    with caplog.at_level(logging.INFO):
        wf._run_nel_stage(_cfg(tmp_path), _args(data_dir=tmp_path), IN_SCOPE,
                          lambda s, f: None, {}, "21")
    assert any("no entities.json available" in r.message for r in caplog.records)


# ---- entity-dump fetch: glitch guard + no-platform ----

def test_fetch_no_platform_is_calm(tmp_path, caplog, monkeypatch):
    import optv.parliaments as op
    monkeypatch.setattr(op, "load_manifest", lambda pid: {}, raising=False)
    with caplog.at_level(logging.INFO):
        wf._run_update_nel_entities_stage(_args(data_dir=tmp_path), "DE")
    assert any("No entity-dump platform configured" in r.message for r in caplog.records)
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_fetch_rejects_empty_dump(tmp_path, caplog, monkeypatch):
    cfg = _cfg(tmp_path)
    _entities(cfg, [{"id": "Q1", "label": "A", "labelAlternative": [], "subType": "person"}])
    before = (cfg.dir('nel_data') / 'entities.json').read_text()
    _patch_urlopen(monkeypatch, json.dumps({"meta": {}, "data": []}).encode())
    with caplog.at_level(logging.WARNING):
        wf._run_update_nel_entities_stage(
            _args(data_dir=tmp_path, nel_entity_url="http://x/dump"), "DE")
    assert "empty" in " ".join(r.message for r in caplog.records).lower()
    assert (cfg.dir('nel_data') / 'entities.json').read_text() == before


def test_fetch_rejects_implausible_collapse(tmp_path, caplog, monkeypatch):
    cfg = _cfg(tmp_path)
    big = [{"id": f"Q{i}", "label": f"L{i}", "labelAlternative": [], "subType": "person"}
           for i in range(10)]
    _entities(cfg, big)
    before = (cfg.dir('nel_data') / 'entities.json').read_text()
    _patch_urlopen(monkeypatch, json.dumps({"meta": {}, "data": big[:2]}).encode())
    with caplog.at_level(logging.WARNING):
        wf._run_update_nel_entities_stage(
            _args(data_dir=tmp_path, nel_entity_url="http://x/dump"), "DE")
    assert "collapsed" in " ".join(r.message for r in caplog.records).lower()
    assert (cfg.dir('nel_data') / 'entities.json').read_text() == before


def test_fetch_force_overrides_collapse(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    big = [{"id": f"Q{i}", "label": f"L{i}", "labelAlternative": [], "subType": "person"}
           for i in range(10)]
    _entities(cfg, big)
    _patch_urlopen(monkeypatch, json.dumps({"meta": {}, "data": big[:2]}).encode())
    wf._run_update_nel_entities_stage(
        _args(data_dir=tmp_path, nel_entity_url="http://x/dump", force=True), "DE")
    after = json.loads((cfg.dir('nel_data') / 'entities.json').read_text())
    assert len(after["data"]) == 2
