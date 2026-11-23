"""A scaffolded parliament: what is laid out, and that the skeleton holds up as a parliament before anything is implemented."""

import importlib
import runpy
import shutil
import sys
import warnings

import pytest

import optv.parliaments
from optv.parliaments import get_locale, get_rights, list_parliaments, load_manifest
from optv.scripts.readiness import scaffold, scorecard
from optv.scripts.readiness.config import load_declaration
from optv.shared import workflow as shared_workflow
from optv.shared.config import BaseConfig

CODE = "ZZ-T"
ARGS = dict(name="Parliament of Testland", parliament_type="regional", language="deu", language_code="de",
            spacy_model="de_core_news_md", periods=[7, 8],
            media_license='Free use with the credit "Parliament of Testland": https://example.org/terms')


@pytest.fixture
def roots(tmp_path, monkeypatch):
    """A Tools tree and a directory of sibling repositories of their own, with the new parliament importable from there."""
    tools, data_root = tmp_path / "tools", tmp_path / "repos"
    (tools / "optv" / "parliaments").mkdir(parents=True)
    (tools / "tests").mkdir()
    data_root.mkdir()
    monkeypatch.setattr(optv.parliaments, "_PARLIAMENTS_DIR", tools / "optv" / "parliaments")
    monkeypatch.setattr(optv.parliaments, "__path__",
                        [*optv.parliaments.__path__, str(tools / "optv" / "parliaments")])
    load_manifest.cache_clear()
    yield tools, data_root
    load_manifest.cache_clear()
    for name in [name for name in sys.modules if name.startswith(f"optv.parliaments.{CODE}")]:
        del sys.modules[name]


def _scaffold(roots, **overrides):
    tools, data_root = roots
    return scaffold.scaffold(CODE, tools=tools, data_root=data_root, **{**ARGS, **overrides})


def test_the_directory_and_the_data_repository_are_laid_out(roots):
    tools, data_root = roots
    report = _scaffold(roots)
    directory = tools / "optv" / "parliaments" / CODE
    assert report["files"] == sorted(path.relative_to(directory).as_posix()
                                     for path in directory.rglob("*") if path.is_file())
    assert {"manifest.yaml", "workflow.py", "common.py", "README.md", "scraper/fetch.py",
            "parsers/media2json.py", "parsers/proceedings2json.py", "merger/merge_session.py"} <= set(report["files"])
    assert not list(directory.rglob("*.tmpl"))

    data_dir = data_root / f"OpenParliamentTV-Data-{CODE}"
    for relative in ("original/media", "original/proceedings", "processed", "metadata"):
        assert (data_dir / relative).is_dir()
    assert "cache/" in (data_dir / ".gitignore").read_text()
    assert (data_dir / "README.md").read_text().splitlines()[:4] == [
        f"# OpenParliamentTV-Data-{CODE}", "Open Parliament TV Data Repository for Parliament of Testland",
        "", "> **Status:** in development"]
    if shutil.which("git"):
        assert (data_dir / ".git").is_dir()
    assert report["research_profile"] is False and report["conductor_entry"] is False
    assert any("Outward-facing, for a person to do" in step for step in report["next"])
    assert f"Created {directory}" in scaffold.render_report(report)


def test_the_manifest_is_complete_and_declares_no_samples_yet(roots):
    _scaffold(roots, aeneas_language="ger", entityfishing_language="en")
    manifest = load_manifest(CODE)
    assert (manifest["name"], manifest["language"], manifest["language_code"]) == (
        "Parliament of Testland", "deu", "de")
    assert manifest["periods"] == [7, 8]
    assert get_locale(CODE) == {"spacy_model": "de_core_news_md", "aeneas_language": "ger",
                                "entityfishing_language": "en"}
    for key in ("supported_stages", "entity_dump_url", "default_retry_count"):
        assert key in manifest
    assert get_rights(CODE, stream="media") == {
        "creator": "Parliament of Testland",
        "license": 'Free use with the credit "Parliament of Testland": https://example.org/terms'}
    assert get_rights(CODE, stream="proceedings") == {"creator": "Parliament of Testland", "license": ""}
    declaration = load_declaration(CODE)
    assert declaration.errors == [] and declaration.samples == []
    assert list_parliaments() == [CODE]


def test_the_skeleton_meets_the_surface_the_conductor_depends_on(roots, tmp_path, monkeypatch, capsys):
    """What tests/test_conductor_contract.py and tests/test_stage_cli.py ask of every parliament."""
    _scaffold(roots)
    common = importlib.import_module(f"optv.parliaments.{CODE}.common")
    config = common.Config(tmp_path / "data")
    assert isinstance(config, BaseConfig)
    assert config.sessions() == [] and config.status("00000") == set()
    assert hasattr(common.SessionStatus, "aligned")

    captured = {}
    monkeypatch.setattr(shared_workflow, "run_workflow",
                        lambda config, args, hooks: captured.update(args=args, hooks=hooks))
    monkeypatch.setattr(sys, "argv", ["workflow", str(tmp_path / "data"), "--no-single-instance",
                                      "--limit-session=^(00001)$", "--merge-speeches", "--link-entities"])
    importlib.import_module(f"optv.parliaments.{CODE}.workflow").main()
    assert captured["hooks"].parliament_id == CODE
    assert callable(captured["hooks"].merge_session_to_file)
    assert captured["args"].period == 8
    assert captured["args"].spacy_model == "de_core_news_md"

    with pytest.raises(NotImplementedError, match=f"{CODE}: merging a session is not implemented yet"):
        captured["hooks"].merge_session_to_file(config, "00001", captured["args"])

    monkeypatch.setattr(sys, "argv", ["merge_session", "--help"])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        with pytest.raises(SystemExit) as exit_info:
            runpy.run_module(f"optv.parliaments.{CODE}.merger.merge_session", run_name="__main__")
    assert exit_info.value.code == 0
    assert "usage" in capsys.readouterr().out.lower()


def test_a_scaffolded_parliament_is_tracked_at_level_one(roots):
    tools, data_root = roots
    _scaffold(roots)
    card = scorecard.evaluate(CODE, tools=tools, data_dir=data_root / f"OpenParliamentTV-Data-{CODE}",
                              previous={})
    assert (card["level"], card["status"]) == (1, "in development")
    for criterion in ("impl.files", "impl.manifest", "impl.contract"):
        assert card["criteria"][criterion]["status"] == "pass", criterion
    assert card["criteria"]["impl.tests"]["status"] == "fail"
    assert card["criteria"]["samples.declared"]["detail"] == "0 declared, 3 needed"


TABLE = """# Tools

| Parliament | Name | Type | Data Repository | Status |
|------------|------|------|-----------------|--------|
| [`DE`](optv/parliaments/DE/) | Deutscher Bundestag | national | [Data-DE](https://example.org/DE) | **production** |
| [`DE-BW`](optv/parliaments/DE-BW/) | Landtag von Baden-Württemberg | regional | [Data-DE-BW](https://example.org/DE-BW) | in development |
| [`FI`](optv/parliaments/FI/) | Eduskunta | national | [Data-FI](https://example.org/FI) | in development |

After the table.
"""


@pytest.mark.parametrize("code, after", [("AT", "DE"), ("DE-ST", "DE-BW"), ("ES", "DE-BW"), ("TW", "FI")])
def test_the_parliament_is_listed_among_those_in_development_by_code(tmp_path, code, after):
    """The table has the parliaments in production first; a new one is in development."""
    readme = tmp_path / "README.md"
    readme.write_text(TABLE)
    assert scaffold.add_readme_row(readme, code, "Some Parliament", "national") == "added to the README table"
    lines = readme.read_text().splitlines()
    row = (f"| [`{code}`](optv/parliaments/{code}/) | Some Parliament | national "
           f"| [Data-{code}](https://github.com/OpenParliamentTV/OpenParliamentTV-Data-{code}) | in development |")
    index = lines.index(row)
    assert lines[index - 1].startswith(f"| [`{after}`]")
    assert "\n".join(lines[:index] + lines[index + 1:]) + "\n" == TABLE
    # A second time changes nothing.
    assert scaffold.add_readme_row(readme, code, "Some Parliament", "national") == "already listed in the README table"
    assert readme.read_text().splitlines() == lines


def test_a_readme_without_the_table_is_left_alone_and_the_step_is_named(roots):
    tools, _ = roots
    (tools / "README.md").write_text("# Tools\n\nNo table here.\n")
    report = _scaffold(roots)
    assert report["readme"] == "README.md has no table of implemented parliaments; add the parliament there by hand"
    assert (tools / "README.md").read_text() == "# Tools\n\nNo table here.\n"
    assert f"List {CODE} in the table of implemented parliaments in README.md." in report["next"]


def test_scaffolding_lists_the_parliament_in_the_readme(roots):
    tools, _ = roots
    (tools / "README.md").write_text(TABLE)
    report = _scaffold(roots)
    assert report["readme"] == "added to the README table"
    assert f"| [`{CODE}`](optv/parliaments/{CODE}/) | Parliament of Testland | regional |" in (tools / "README.md").read_text()
    assert not any("table of implemented parliaments" in step for step in report["next"])
    with pytest.raises(scaffold.ScaffoldError, match="the type is one of"):
        scaffold.scaffold("ZZ-U", tools=tools, data_root=roots[1], **{**ARGS, "parliament_type": "municipal"})


def test_nothing_is_written_on_a_dry_run(roots):
    tools, data_root = roots
    (tools / "README.md").write_text(TABLE)
    report = _scaffold(roots, dry_run=True)
    assert report["readme"] == "would be added to the README table"
    assert (tools / "README.md").read_text() == TABLE
    assert report["dry_run"] and "workflow.py" in report["files"]
    assert not (tools / "optv" / "parliaments" / CODE).exists()
    assert not (data_root / f"OpenParliamentTV-Data-{CODE}").exists()
    assert scaffold.render_report(report).startswith("Would create")


def test_what_exists_is_never_overwritten(roots):
    tools, data_root = roots
    data_dir = data_root / f"OpenParliamentTV-Data-{CODE}"
    (data_dir / "processed").mkdir(parents=True)
    (data_dir / "README.md").write_text("mine")
    report = _scaffold(roots)
    assert report["data_repo"] == ["exists already, left as it is"]
    assert (data_dir / "README.md").read_text() == "mine"
    with pytest.raises(scaffold.ScaffoldError, match="exists already"):
        _scaffold(roots)


def test_the_data_repository_template_is_used_when_it_is_there(roots):
    tools, data_root = roots
    template = data_root / "_TEMPLATES" / "DATA-REPO"
    (template / "metadata").mkdir(parents=True)
    (template / ".gitignore").write_text("cache/\nfrom-the-template\n")
    (template / ".DS_Store").write_text("x")
    _scaffold(roots)
    data_dir = data_root / f"OpenParliamentTV-Data-{CODE}"
    assert "from-the-template" in (data_dir / ".gitignore").read_text()
    assert not (data_dir / ".DS_Store").exists()
    assert (data_dir / "original" / "media").is_dir()


def test_existing_surroundings_are_noticed(roots):
    tools, data_root = roots
    profile = data_root / "OpenParliamentTV-Research" / "parliaments" / "ZZ" / f"{CODE}.md"
    profile.parent.mkdir(parents=True)
    profile.write_text("# profile")
    conductor = data_root / "OpenParliamentTV-Conductor" / "config" / "parliaments.yaml"
    conductor.parent.mkdir(parents=True)
    conductor.write_text(f"parliaments:\n  DE:\n    stages: {{}}\n  {CODE}:\n    stages: {{}}\n")
    report = _scaffold(roots)
    assert report["research_profile"] is True and report["conductor_entry"] is True
    assert not any("Conductor:" in step or "No Research profile" in step for step in report["next"])


def test_the_proceedings_licence_is_taken_when_it_is_known(roots):
    _scaffold(roots, proceedings_license="Public Domain")
    assert get_rights(CODE, stream="proceedings")["license"] == "Public Domain"


def test_no_parliament_without_the_terms_of_its_video(roots):
    with pytest.raises(scaffold.ScaffoldError, match="terms of use of the video recordings"):
        _scaffold(roots, media_license="  ")


@pytest.mark.parametrize("code", ["de", "DEU", "DE_BW", "DE-", "D", "DE-ABCD"])
def test_a_code_that_is_no_shortcode_is_refused(roots, code):
    tools, data_root = roots
    with pytest.raises(scaffold.ScaffoldError, match="not a shortcode"):
        scaffold.scaffold(code, tools=tools, data_root=data_root, **ARGS)
