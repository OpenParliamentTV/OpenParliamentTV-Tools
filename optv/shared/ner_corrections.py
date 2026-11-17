"""Corrections to the entities the NER stage links (opt-in per parliament).

entity-fishing links every span spaCy tags as an entity, and some links are wrong in a way no confidence threshold separates ("Srs" in "Srs. Deputados" → Sender Rewriting Scheme; the party abbreviation "CH" → Order of the Companions of Honour). Each kind of error is handled by the key it depends on, all as data:

1. **Mention noise depends on the language.** ``ner_lists/stop_mentions/<lang>.txt`` lists mentions that are never entities (forms of address), keyed by the spaCy model's language. Such a mention is dropped.
2. **Ambiguous names and abbreviations depend on the parliament.** A mention that exactly matches a speaker linked in the same sitting, or a faction label/alias in the parliament's ``metadata/entities.json``, gets that entity's wid. This also links mentions entity-fishing left unlinked. Persons come only from the sitting's own linked speakers, so a namesake elsewhere in the entity dump cannot take over.
3. **Bad targets are wrong in every parliament.** ``ner_lists/excluded_classes.yaml`` lists Wikidata classes (disambiguation pages, letters, given and family names, …); a link whose ``P31`` (instance of) is one of them is dropped. The classes are read from the entity-fishing KB (``/kb/concept/{qid}``).

One-off QID → QID corrections belong in the platform's EntityRemap, which applies them at import. docs/NER-CORRECTIONS.md says where a fix belongs.

A parliament turns the corrections on with ``ner: {corrections: true}`` in its ``manifest.yaml``.
"""

from __future__ import annotations

import collections
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional

import requests

from optv.shared.nel import cleanup, get_nel_data

logger = logging.getLogger(__name__)

LISTS_DIR = Path(__file__).parent / "ner_lists"


def load_stop_mentions(lang: str) -> frozenset[str]:
    """Read ``stop_mentions/<lang>.txt`` (one mention per line, ``#`` starts a comment), normalised like NEL labels. A missing file means no stop list."""
    path = LISTS_DIR / "stop_mentions" / f"{lang}.txt"
    if not lang or not path.is_file():
        return frozenset()
    keys = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        key = cleanup(line.split("#", 1)[0])
        if key:
            keys.add(key)
    return frozenset(keys)


def load_excluded_classes() -> frozenset[str]:
    """Read the QIDs of ``excluded_classes.yaml`` (``QID: label``)."""
    import yaml
    with (LISTS_DIR / "excluded_classes.yaml").open(encoding="utf-8") as f:
        return frozenset((yaml.safe_load(f) or {}).keys())


class ClassLookup:
    """``P31`` classes of a QID from the entity-fishing KB, cached per run.

    Fails open: when the KB cannot be read, the QID counts as having no class (its link is kept) and the problem is logged once.
    """

    def __init__(self, api_base: str, timeout: float = 10.0):
        self._url = api_base.rstrip("/") + "/kb/concept/"
        self._timeout = timeout
        self._cache: dict[str, frozenset[str]] = {}
        self._warned = False

    def __call__(self, qid: str) -> frozenset[str]:
        if qid not in self._cache:
            try:
                response = requests.get(self._url + qid, timeout=self._timeout)
                response.raise_for_status()
                statements = response.json().get("statements") or []
                classes = frozenset(s["value"] for s in statements
                                    if s.get("propertyId") == "P31" and s.get("value"))
            except (requests.RequestException, ValueError, TypeError, KeyError) as e:
                if not self._warned:
                    logger.warning(f"Cannot read entity classes from {self._url} "
                                   f"({type(e).__name__}: {e}); keeping links unfiltered")
                    self._warned = True
                classes = frozenset()
            self._cache[qid] = classes
        return self._cache[qid]


@dataclass
class NerCorrections:
    stop_mentions: frozenset[str] = frozenset()
    factions: dict[str, str] = field(default_factory=dict)   # cleaned label → wid
    class_lookup: Optional[Callable[[str], frozenset[str]]] = None
    excluded_classes: frozenset[str] = frozenset()

    @classmethod
    def build(cls, *, language: str, nel_data_dir: Optional[Path],
              api_base: str) -> "NerCorrections":
        factions: dict[str, str] = {}
        if nel_data_dir is not None and (Path(nel_data_dir) / "entities.json").is_file():
            _, faction_entities = get_nel_data(Path(nel_data_dir))
            factions = {key: ent["id"] for key, ent in faction_entities.items()
                        if key and ent.get("id")}
        corrections = cls(stop_mentions=load_stop_mentions(language),
                      factions=factions,
                      class_lookup=ClassLookup(api_base),
                      excluded_classes=load_excluded_classes())
        logger.info(f"NER corrections: {len(corrections.stop_mentions)} stop mention(s) "
                    f"for {language!r}, {len(factions)} faction label(s), "
                    f"{len(corrections.excluded_classes)} excluded class(es)")
        return corrections

    @staticmethod
    def session_persons(speeches: Iterable[dict]) -> dict[str, str]:
        """Cleaned label → wid of the speakers NEL linked in this sitting."""
        persons: dict[str, str] = {}
        for speech in speeches:
            for person in speech.get("people") or []:
                key = cleanup(person.get("label"))
                if key and person.get("wid"):
                    persons.setdefault(key, person["wid"])
        return persons

    def apply(self, mentions: Iterable[dict], persons: Optional[dict] = None,
              stats: Optional[collections.Counter] = None) -> list[dict]:
        """Filter one sentence's mentions (``{label, wid, wtype, score}``, ``wid`` ``None`` where entity-fishing linked nothing); returns the entities to keep.

        ``stats`` counts ``stop`` (dropped stop mentions), ``relinked`` / ``rescued`` (wid taken from the sitting's speakers or the factions, replacing a different link / where there was none) and ``class`` (dropped excluded-class links).
        """
        if stats is None:
            stats = collections.Counter()
        kept = []
        for mention in mentions:
            key = cleanup(mention.get("label")) or ""
            wid = mention.get("wid")
            if key in self.stop_mentions:
                stats["stop"] += 1
                continue
            listed = (persons or {}).get(key) or self.factions.get(key)
            if listed:
                if listed != wid:
                    stats["relinked" if wid else "rescued"] += 1
                    mention = {**mention, "wid": listed, "score": None}
                kept.append(mention)
                continue
            if not wid:
                continue
            if self.class_lookup is not None and self.class_lookup(wid) & self.excluded_classes:
                stats["class"] += 1
                continue
            kept.append(mention)
        return kept
