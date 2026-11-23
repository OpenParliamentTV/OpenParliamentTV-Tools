"""Keeping the hand-written status entries in line with the scorecards: the README table, the data repository READMEs and the Research profiles."""

import json

import pytest

from optv.scripts.readiness import sync

README = """# Tools

## Implemented parliaments

| Parliament | Name | Type | Data Repository | Status |
|---|---|---|---|---|
| [`DE`](optv/parliaments/DE/) | Deutscher Bundestag | national | [Data-DE](https://example.org/DE) | **production** |
| [`DE-BW`](optv/parliaments/DE-BW/) | Landtag von Baden-Württemberg | regional | [Data-DE-BW](https://example.org/DE-BW) | in development |
| [`FI`](optv/parliaments/FI/) | Eduskunta | national | [Data-FI](https://example.org/FI) | in development |

More text.
"""


def _data_readme(code, status):
    return f"# OpenParliamentTV-Data-{code}\nData of {code}\n\n> **Status:** {status}\n\n## Pipeline\n"


def _profile(code, status):
    return (f"# {code}\n\n## Summary\n\n| Field | Value |\n|---|---|\n| **Type** | national |\n"
            f"| **OPTV Status** | {status} |\n| **Integration Feasibility** | 12/23 |\n")


@pytest.fixture
def roots(tmp_path):
    """Three tracked parliaments whose status entries all agree with their scorecards."""
    tools, data_root = tmp_path / "tools", tmp_path / "repos"
    (tools / "docs" / "readiness").mkdir(parents=True)
    (tools / "README.md").write_text(README)
    for code, status in (("DE", "production"), ("DE-BW", "in development"), ("FI", "in development")):
        (tools / "docs" / "readiness" / f"{code}.json").write_text(json.dumps(
            {"parliament": code, "status": status, "level": 5 if status == "production" else 1}))
        data_dir = data_root / f"OpenParliamentTV-Data-{code}"
        data_dir.mkdir(parents=True)
        (data_dir / "README.md").write_text(_data_readme(code, status))
        profile = data_root / "OpenParliamentTV-Research" / "parliaments" / code.split("-")[0] / f"{code}.md"
        profile.parent.mkdir(parents=True, exist_ok=True)
        profile.write_text(_profile(code, "Operational" if status == "production" else "In development"))
    return tools, data_root


def _promote(tools, code):
    (tools / "docs" / "readiness" / f"{code}.json").write_text(json.dumps(
        {"parliament": code, "status": "production", "level": 5}))


def test_places_that_agree_are_left_alone(roots):
    assert sync.findings(*roots) == []


def test_a_promoted_parliament_is_out_of_line_in_all_three_places(roots):
    tools, data_root = roots
    _promote(tools, "FI")
    found = sync.findings(tools, data_root)
    assert [(f.parliament, f.place, f.found, f.expected) for f in found] == [
        ("FI", "the Tools README table", "in development", "**production**"),
        ("FI", "the data repository README", "in development", "production"),
        ("FI", "the Research profile", "In development", "Operational")]
    assert found[0].describe() == ("FI: the Tools README table says 'in development', "
                                   "the scorecard gives '**production**'")


def test_apply_rewrites_only_the_value(roots):
    tools, data_root = roots
    _promote(tools, "FI")
    assert sync.apply(sync.findings(tools, data_root)) == []
    assert sync.findings(tools, data_root) == []
    readme = (tools / "README.md").read_text()
    promoted = "| [`FI`](optv/parliaments/FI/) | Eduskunta | national | [Data-FI](https://example.org/FI) | **production** |"
    assert promoted in readme
    # Nothing but the value changed, and the row moved up to the parliaments in production.
    assert sorted(readme.replace("| **production** |\n| [`DE-BW`]", "| in development |\n| [`DE-BW`]").splitlines()) \
        == sorted(README.splitlines())
    assert [line.split("`")[1] for line in readme.splitlines() if line.startswith("| [`")] == ["DE", "FI", "DE-BW"]
    assert readme.endswith("\nMore text.\n")
    assert (data_root / "OpenParliamentTV-Data-FI" / "README.md").read_text() == _data_readme("FI", "production")
    profile = data_root / "OpenParliamentTV-Research" / "parliaments" / "FI" / "FI.md"
    assert profile.read_text() == _profile("FI", "Operational")
    # The other parliaments are untouched.
    assert (data_root / "OpenParliamentTV-Data-DE-BW" / "README.md").read_text() == _data_readme("DE-BW", "in development")


def test_a_parliament_that_fell_back_is_set_to_in_development(roots):
    tools, data_root = roots
    (tools / "docs" / "readiness" / "DE.json").write_text(json.dumps(
        {"parliament": "DE", "status": "in development", "level": 3}))
    found = sync.findings(tools, data_root)
    assert [(f.found, f.expected) for f in found] == [
        ("**production**", "in development"), ("production", "in development"),
        ("Operational", "In development")]


def test_the_table_is_ordered_by_status_then_code():
    shuffled = README.replace(
        "| [`DE`](optv/parliaments/DE/) | Deutscher Bundestag | national | [Data-DE](https://example.org/DE) | **production** |\n", "").replace(
        "\n\nMore text.", "\n| [`DE`](optv/parliaments/DE/) | Deutscher Bundestag | national | [Data-DE](https://example.org/DE) | **production** |\n\nMore text.")
    assert shuffled != README
    assert sync.sort_readme_table(shuffled) == README
    assert sync.sort_readme_table(README) == README
    assert sync.sort_readme_table("# no table\n") == "# no table\n"


def test_a_missing_entry_is_reported_and_left_for_a_person(roots):
    tools, data_root = roots
    (tools / "docs" / "readiness" / "SE.json").write_text(json.dumps(
        {"parliament": "SE", "status": "in development", "level": 1}))
    data_dir = data_root / "OpenParliamentTV-Data-SE"
    data_dir.mkdir()
    (data_dir / "README.md").write_text("# OpenParliamentTV-Data-SE\nno status here\n")
    found = sync.findings(tools, data_root)
    assert [(f.place, f.found, f.fixable) for f in found] == [
        ("the Tools README table", None, False), ("the data repository README", None, False)]
    assert found[0].describe() == "SE: the Tools README table has no status entry (expected 'in development')"
    assert sync.apply(found) == found
    assert (data_dir / "README.md").read_text() == "# OpenParliamentTV-Data-SE\nno status here\n"


def test_repositories_that_are_not_here_are_passed_over(roots, tmp_path):
    tools, _ = roots
    assert sync.findings(tools, tmp_path / "nowhere") == []
