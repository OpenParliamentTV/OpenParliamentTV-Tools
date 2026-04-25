from functools import lru_cache
from pathlib import Path
from typing import Any

_PARLIAMENTS_DIR = Path(__file__).parent


def manifest_path(parliament_id: str) -> Path:
    return _PARLIAMENTS_DIR / parliament_id / "manifest.yaml"


def list_parliaments() -> list[str]:
    return sorted(
        p.name
        for p in _PARLIAMENTS_DIR.iterdir()
        if p.is_dir() and (p / "manifest.yaml").is_file()
    )


@lru_cache(maxsize=None)
def load_manifest(parliament_id: str) -> dict[str, Any]:
    import yaml

    path = manifest_path(parliament_id)
    if not path.is_file():
        raise FileNotFoundError(f"No manifest for parliament {parliament_id!r} at {path}")
    with path.open() as f:
        return yaml.safe_load(f)
