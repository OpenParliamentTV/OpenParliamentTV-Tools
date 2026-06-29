"""Source selection + gating for the in-place enrichment stages (NEL / NER).

These lock in the fix for the stale-cache class of bug: a media-only `aligned`
stub (left over from a run where proceedings were briefly unavailable) used to
shadow a fully transcribed published session, so NER ran over text-less input
and the publish guard silently dropped the result on every run. The general
rule is now: source from `processed/` (the git-tracked high-water mark) when it
exists, and gate on the file's own `meta.processing.<stage>` rather than on
local cache mtimes.
"""

import json
from types import SimpleNamespace

from optv.shared.workflow import (
    _enrichment_is_current,
    _enrichment_source,
    _nel_is_current,
    _publish_as_processed,
)


class _FakeConfig:
    """Minimal stand-in exposing the one method the helpers call."""

    def __init__(self, root):
        self.root = root

    def file(self, session, stage="processed", create=False):
        return self.root / f"{session}-{stage}.json"


def _touch(path, processing=None):
    path.write_text(
        json.dumps({"meta": {"processing": processing or {}}, "data": []}),
        encoding="utf-8",
    )


def test_enrichment_source_prefers_processed_over_cache(tmp_path):
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21020", "aligned"))      # stale media-only stub on disk
    _touch(cfg.file("21020", "processed"))    # the published truth
    assert _enrichment_source(cfg, "21020") == cfg.file("21020", "processed")


def test_enrichment_source_falls_back_to_freshest_cache_before_first_publish(tmp_path):
    cfg = _FakeConfig(tmp_path)
    _touch(cfg.file("21020", "merged"))
    _touch(cfg.file("21020", "aligned"))
    # No processed yet -> richest existing cache (aligned beats merged).
    assert _enrichment_source(cfg, "21020") == cfg.file("21020", "aligned")


def test_enrichment_source_returns_merged_path_when_nothing_exists(tmp_path):
    cfg = _FakeConfig(tmp_path)
    assert _enrichment_source(cfg, "21020") == cfg.file("21020", "merged")


def test_ner_runs_when_published_file_has_no_ner_pass(tmp_path):
    """The reported case: processed/ freshly merged+aligned but never NER'd."""
    src = tmp_path / "21074-session.json"
    _touch(src, {"merge": "2026-06-18T17:15:49", "align": "2026-06-18T17:42:49"})
    assert _enrichment_is_current(src, "ner") is False


def test_ner_skipped_when_pass_is_newer_than_upstream(tmp_path):
    src = tmp_path / "21079-session.json"
    _touch(src, {
        "merge": "2026-05-21T11:15:28",
        "align": "2026-05-21T11:15:54",
        "ner": "2026-06-19T05:04:53",
    })
    assert _enrichment_is_current(src, "ner") is True


def test_ner_reruns_when_remerge_advances_past_pass(tmp_path):
    src = tmp_path / "21074-session.json"
    _touch(src, {
        "merge": "2026-06-18T17:15:49",
        "ner": "2026-05-15T16:54:00",  # pre-remerge NER
    })
    assert _enrichment_is_current(src, "ner") is False


def test_nel_not_rerun_just_because_align_is_newer(tmp_path):
    """The reported empty-every-cron case: nel always stamps before align in a
    run, so align is permanently newer. NEL only depends on merge, so this must
    still count as current and skip."""
    src = tmp_path / "21045-session.json"
    _touch(src, {
        "merge": "2025-12-01T09:15:27",
        "nel": "2025-12-01T09:15:30",
        "align": "2025-12-01T09:15:34",  # newer than nel, but irrelevant to NEL
    })
    assert _nel_is_current(src) is True


def test_nel_reruns_when_remerge_advances_past_pass(tmp_path):
    src = tmp_path / "21074-session.json"
    _touch(src, {
        "merge": "2026-06-18T17:15:49",  # re-merge after last nel
        "nel": "2025-12-01T09:15:30",
        "align": "2025-12-01T09:15:34",
    })
    assert _nel_is_current(src) is False


# --rebuild publish behaviour: the current run is authoritative for the
# stage-derived enrichments (a removed agendaItem.type / confidence / documents
# stays removed), so their carry-forward stands down -- but the publish still
# happens (not a demotion) because the transcript is unchanged. wids are the
# exception: they are owned solely by NEL (which re-derives them in place under
# --rebuild), so the fill-only wid carry-forward runs even under --rebuild to
# stop a non-NEL stage from silently dropping committed wids.

def _speech_with_text(origin, people, debug=None):
    return {
        "originTextID": origin,
        "people": people,
        "debug": debug or {},
        "textContents": [{"textBody": [{"sentences": [{"text": "Hallo Welt."}]}]}],
    }


def _write_doc(path, data):
    path.write_text(json.dumps({"meta": {"processing": {}}, "data": data}),
                    encoding="utf-8")


def test_publish_carries_wid_forward_without_rebuild(tmp_path):
    cfg = _FakeConfig(tmp_path)
    processed = cfg.file("21001", "processed")
    _write_doc(processed, [_speech_with_text(
        "A", [{"label": "X", "wid": "Q1", "wtype": "PERSON"}], debug={"nerDuration": 1.0})])
    new = cfg.file("21001", "ner")
    _write_doc(new, [_speech_with_text("A", [{"label": "X"}], debug={"nerDuration": 2.0})])

    _publish_as_processed(cfg, SimpleNamespace(rebuild=False, validate=False), "21001", new)

    result = json.loads(processed.read_text())
    assert result["data"][0]["people"][0]["wid"] == "Q1"  # restored from published


def test_publish_carries_wid_forward_even_under_rebuild(tmp_path):
    # wids are owned exclusively by NEL; a non-NEL stage (here a bare NER
    # republish) must never drop a committed wid, even under --rebuild. The
    # fill-only carry-forward can't resurrect a wid NEL intentionally removed,
    # because a rebuilt NEL re-derives in place so new == published there.
    cfg = _FakeConfig(tmp_path)
    processed = cfg.file("21001", "processed")
    _write_doc(processed, [_speech_with_text(
        "A", [{"label": "X", "wid": "Q1", "wtype": "PERSON"}], debug={"nerDuration": 1.0})])
    new = cfg.file("21001", "ner")
    _write_doc(new, [_speech_with_text("A", [{"label": "X"}], debug={"nerDuration": 2.0})])

    _publish_as_processed(cfg, SimpleNamespace(rebuild=True, validate=False), "21001", new)

    result = json.loads(processed.read_text())
    assert result["data"][0]["people"][0]["wid"] == "Q1"  # restored from published


def test_publish_does_not_carry_enrichment_forward_under_rebuild(tmp_path):
    # Non-wid enrichments (agendaItem.type, debug.confidence, documents) are
    # re-derived by the rebuilt stages themselves, so --rebuild makes the
    # current run authoritative: a removed value stays removed.
    cfg = _FakeConfig(tmp_path)
    processed = cfg.file("21001", "processed")
    _write_doc(processed, [_speech_with_text(
        "A", [{"label": "X"}], debug={"nerDuration": 1.0, "confidence": 0.9})])
    new = cfg.file("21001", "ner")
    _write_doc(new, [_speech_with_text("A", [{"label": "X"}], debug={"nerDuration": 2.0})])

    _publish_as_processed(cfg, SimpleNamespace(rebuild=True, validate=False), "21001", new)

    result = json.loads(processed.read_text())
    assert "confidence" not in result["data"][0]["debug"]  # removal persists
