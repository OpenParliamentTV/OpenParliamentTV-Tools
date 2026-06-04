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
    """Load the parliament's manifest.yaml.

    Recognised top-level keys: ``name``, ``language``, ``locale``, ``periods``,
    ``supported_stages``, ``entity_dump_url``, ``default_retry_count``,
    ``default_retry_delay_max``. ``locale`` is a sub-mapping with
    ``spacy_model``, ``aeneas_language``, ``entityfishing_language`` (consumed
    by ``optv.shared.ner`` / ``optv.shared.align``).
    """
    import yaml

    path = manifest_path(parliament_id)
    if not path.is_file():
        raise FileNotFoundError(f"No manifest for parliament {parliament_id!r} at {path}")
    with path.open() as f:
        return yaml.safe_load(f)


def get_rights(parliament_id: str, period: int = None, stream: str = "media") -> dict:
    """Resolve the creator/license/source for a ``(period, stream)``.

    Reads the manifest ``media`` / ``proceedings`` block, which has the shape::

        <stream>:
          default: {creator?, license?, source?, sourceURI?}
          overrides:
            - periods: [16, 17]
              creator: ...
              license: ...

    Returns ``default`` shallow-merged with the first ``override`` whose
    ``periods`` list contains ``period`` (``period=None`` ⇒ default only).
    Keys the manifest omits are absent from the result, so callers keep their
    data-driven fallback (e.g. DE media ``creator`` = feed author, DE native
    proceedings ``creator`` = TEI ``<herausgeber>``). Rare per-period
    rights-holder changes on *either* stream are expressed via ``overrides``.
    """
    block = load_manifest(parliament_id).get(stream) or {}
    result = dict(block.get("default") or {})
    if period is not None:
        for override in block.get("overrides") or []:
            if period in (override.get("periods") or []):
                result.update({k: v for k, v in override.items() if k != "periods"})
                break
    return result


def get_language(parliament_id: str) -> str:
    """The Stage-2 emission language code (``originalLanguage`` /
    ``textContents[].language``).

    Per ``OpenParliamentTV-Architecture/SHORTCODES.md`` §3 this is **ISO 639
    Alpha-2, lowercase** (``de``/``es``/``sv``/…) for every parliament. Distinct
    from the manifest's ISO-639-3 ``language`` (``deu``) used by the locale
    stack. Read from the manifest (``language_code``) so each parliament pins
    its exact code.
    """
    return load_manifest(parliament_id).get("language_code", "")


_LOCALE_REQUIRED_KEYS = ("spacy_model", "aeneas_language", "entityfishing_language")


def get_locale(parliament_id: str) -> dict[str, str]:
    """Return the manifest's ``locale`` block.

    Raises ``KeyError`` with a message pointing at ``manifest.yaml`` if the
    block is missing or any required key (``spacy_model``, ``aeneas_language``,
    ``entityfishing_language``) is absent. Callers should invoke this lazily
    (only when they actually need NER/alignment locale config).
    """
    manifest = load_manifest(parliament_id)
    locale = manifest.get("locale")
    path = manifest_path(parliament_id)
    if not isinstance(locale, dict):
        raise KeyError(
            f"Manifest {path} has no 'locale' block. Add one with keys: "
            f"{', '.join(_LOCALE_REQUIRED_KEYS)}."
        )
    missing = [k for k in _LOCALE_REQUIRED_KEYS if not locale.get(k)]
    if missing:
        raise KeyError(
            f"Manifest {path} 'locale' block is missing required key(s): "
            f"{', '.join(missing)}."
        )
    return locale
