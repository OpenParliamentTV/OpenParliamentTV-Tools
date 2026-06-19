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

from optv.shared.workflow import _enrichment_source, _enrichment_is_current


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
