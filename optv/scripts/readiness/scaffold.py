"""Lay out a new parliament: its directory in this repository and a local data repository beside it.

The skeleton imports, parses the Conductor's arguments and passes the contract tests from the first moment; its scraper, parsers and merger raise ``NotImplementedError`` until they are written. Nothing here leaves the machine: no GitHub repository is created, nothing is committed or pushed.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path
from string import Template

from . import config as cfg
from .sync import README_ROW, sort_readme_table

TEMPLATES = Path(__file__).parent / "templates"
DATA_TEMPLATE = "_TEMPLATES/DATA-REPO"
DATA_README = "data-README.md.tmpl"
DATA_DIRS = ("original/media", "original/proceedings", "processed", "metadata")
CODE = re.compile(r"[A-Z]{2}(-[A-Z0-9]{1,3})?")
TYPES = ("national", "regional", "supranational")

# The .gitignore of a data repository, for a machine without the template beside it.
DATA_GITIGNORE = """# Do not version converted/parsed media files (originals are raw-*/source formats)
original/media/[0-9]*-media.json
# Do not version converted/parsed proceeding files (originals are xml/html/jsonld/raw-*)
original/proceedings/[0-9]*-proceedings.json

# Do not version cache directory
cache/

.DS_Store
__pycache__
*.log
*.lock
"""


class ScaffoldError(ValueError):
    pass


def _render(template: Path, values: dict) -> str:
    return Template(template.read_text()).substitute(values)


def parliament_files(values: dict) -> dict[str, str]:
    """Relative path -> content of every file of a new parliament directory."""
    files = {}
    for template in sorted(TEMPLATES.rglob("*.tmpl")):
        if template.name == DATA_README:
            continue
        relative = template.relative_to(TEMPLATES).as_posix()[:-len(".tmpl")]
        files[relative] = _render(template, values)
    return files


def _create_data_repo(directory: Path, data_root: Path, values: dict) -> list[str]:
    done = []
    template = data_root / DATA_TEMPLATE
    if template.is_dir():
        shutil.copytree(template, directory, ignore=shutil.ignore_patterns(".DS_Store", ".git"))
        done.append(f"copied {DATA_TEMPLATE}")
    for relative in DATA_DIRS:
        path = directory / relative
        path.mkdir(parents=True, exist_ok=True)
        if not any(path.iterdir()):
            (path / ".gitkeep").write_text("")
    if not (directory / ".gitignore").exists():
        (directory / ".gitignore").write_text(DATA_GITIGNORE)
    (directory / "README.md").write_text(_render(TEMPLATES / DATA_README, values))
    done.append("wrote README.md with the status line")
    if shutil.which("git"):
        cp = subprocess.run(["git", "init", "-q", "-b", "main", str(directory)], capture_output=True, text=True)
        done.append("initialised a local git repository" if cp.returncode == 0
                    else f"git init failed: {cp.stderr.strip()}")
    return done


def readme_row(code: str, name: str, parliament_type: str) -> str:
    repo = f"{cfg.DATA_REPO_PREFIX}{code}"
    return (f"| [`{code}`](optv/parliaments/{code}/) | {name} | {parliament_type} "
            f"| [Data-{code}](https://github.com/OpenParliamentTV/{repo}) | {cfg.STATUS_DEVELOPMENT} |")


def add_readme_row(readme: Path, code: str, name: str, parliament_type: str, *, dry_run: bool = False) -> str:
    """List the parliament in the README's table of implemented parliaments, which is ordered by status (production first) and then by code. Says what it did."""
    if not readme.is_file():
        return "no README.md to list the parliament in"
    text = readme.read_text()
    rows = list(README_ROW.finditer(text))
    if not rows:
        return "README.md has no table of implemented parliaments; add the parliament there by hand"
    if any(row.group(1) == code for row in rows):
        return "already listed in the README table"
    if dry_run:
        return "would be added to the README table"
    position = rows[-1].end()
    text = text[:position] + "\n" + readme_row(code, name, parliament_type) + text[position:]
    readme.write_text(sort_readme_table(text))
    return "added to the README table"


def scaffold(code: str, *, name: str, parliament_type: str, language: str, language_code: str,
             spacy_model: str, periods: list[int], media_license: str, proceedings_license: str = "",
             aeneas_language: str | None = None, entityfishing_language: str | None = None,
             tools: Path | None = None, data_root: Path | None = None, dry_run: bool = False) -> dict:
    """Create the parliament directory and the local data repository. Returns what was done and what is left for a person.

    ``media_license`` is the terms of use of the video recordings, as wording or as the address of the parliament's page: no parliament is in this repository without it.
    """
    tools = tools or cfg.tools_root()
    data_root = data_root or cfg.data_root()
    if not CODE.fullmatch(code):
        raise ScaffoldError(f"{code!r} is not a shortcode like FI or DE-BW (see SHORTCODES.md)")
    if parliament_type not in TYPES:
        raise ScaffoldError(f"the type is one of {', '.join(TYPES)}")
    if not periods:
        raise ScaffoldError("at least one period is needed")
    if not media_license.strip():
        raise ScaffoldError("the terms of use of the video recordings are needed (media licence)")
    target = tools / "optv" / "parliaments" / code
    if target.exists():
        raise ScaffoldError(f"{target} exists already")
    values = {
        "code": code, "name": name, "language": language, "language_code": language_code,
        "spacy_model": spacy_model, "aeneas_language": aeneas_language or language,
        "entityfishing_language": entityfishing_language or language_code,
        "periods": ", ".join(str(p) for p in sorted(periods)), "newest_period": max(periods),
        # JSON strings are YAML double-quoted scalars.
        "name_yaml": json.dumps(name, ensure_ascii=False),
        "media_license_yaml": json.dumps(media_license.strip(), ensure_ascii=False),
        "proceedings_license_yaml": (json.dumps(proceedings_license.strip(), ensure_ascii=False)
                                     if proceedings_license.strip()
                                     else '""    # TODO the terms of use of the proceedings'),
    }
    files = parliament_files(values)
    data_dir = data_root / f"{cfg.DATA_REPO_PREFIX}{code}"
    report = {"parliament": code, "directory": str(target), "files": sorted(files),
              "data_dir": str(data_dir), "data_repo": [], "dry_run": dry_run}
    if not dry_run:
        for relative, content in files.items():
            path = target / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    if data_dir.exists():
        report["data_repo"].append("exists already, left as it is")
    elif dry_run:
        report["data_repo"].append("would be created")
    else:
        report["data_repo"] = _create_data_repo(data_dir, data_root, values)

    report["readme"] = add_readme_row(tools / "README.md", code, name, parliament_type, dry_run=dry_run)

    country = code.split("-")[0]
    profile = data_root / "OpenParliamentTV-Research" / "parliaments" / country / f"{code}.md"
    report["research_profile"] = profile.is_file()
    conductor = data_root / "OpenParliamentTV-Conductor" / "config" / "parliaments.yaml"
    report["conductor_entry"] = (conductor.is_file()
                                 and re.search(rf"^  {re.escape(code)}:\s*$", conductor.read_text(), re.M) is not None)
    report["next"] = next_steps(code, report)
    return report


def conductor_block(code: str) -> str:
    return (f"  {code}:\n"
            f'    git_remote: "git@github.com:OpenParliamentTV/{cfg.DATA_REPO_PREFIX}{code}.git"\n'
            "    stages:\n"
            "      download: true\n      parse: true\n      merge: true\n"
            "      nel: true\n      align: true\n      ner: false\n")


def next_steps(code: str, report: dict) -> list[str]:
    steps = []
    if not report["research_profile"]:
        steps.append(f"No Research profile for {code} on this machine: survey the source data first "
                     "(docs/ADDING-A-PARLIAMENT.md, section 2).")
    if "by hand" in report["readme"] or "no README" in report["readme"]:
        steps.append(f"List {code} in the table of implemented parliaments in README.md.")
    steps += [
        f"Fill in optv/parliaments/{code}/manifest.yaml: the licence of the proceedings if it is still open, "
        "the stages that do not apply, entity_dump_url.",
        "Implement scraper/fetch.py, parsers/ and merger/merge_session.py; add tests/test_"
        f"{code.lower().replace('-', '_')}*.py.",
        "Process three sample sessions, declare them under readiness.sample_sessions, and declare "
        "readiness.sample_run once the download hook is known to honour the session arguments.",
        f"Score it: python -m optv.scripts.readiness check {code}",
    ]
    if not report["conductor_entry"]:
        steps.append("Conductor: add this block under `parliaments:` in config/parliaments.yaml, mount the data "
                     "repository in the local compose override, and restart:\n" + conductor_block(code))
    steps.append("Outward-facing, for a person to do: create the GitHub repository "
                 f"OpenParliamentTV/{cfg.DATA_REPO_PREFIX}{code}, add it as the remote of the local data "
                 "repository, commit and push both repositories.")
    return steps


def render_report(report: dict) -> str:
    verb = "Would create" if report["dry_run"] else "Created"
    lines = [f"{verb} {report['directory']}:"]
    lines += [f"  {name}" for name in report["files"]]
    lines.append(f"README.md: {report['readme']}")
    lines.append(f"Data repository {report['data_dir']}: {'; '.join(report['data_repo'])}")
    lines.append(f"Research profile: {'found' if report['research_profile'] else 'not found'}; "
                 f"Conductor entry: {'present' if report['conductor_entry'] else 'missing'}")
    lines.append("")
    lines.append("Next:")
    lines += [f"{number}. {step}" for number, step in enumerate(report["next"], start=1)]
    return "\n".join(lines)
