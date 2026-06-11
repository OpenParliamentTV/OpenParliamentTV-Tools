"""DE-BW (Landtag von Baden-Württemberg) PDF→TEI detection config.

Lifted from the prototype. BW prints "Abg. Name FRAKTION:" and ministers with
the full ministry in the label. The spine keeps a whole back-and-forth (incl.
Zwischenfragen) in one clip, so the rede-merge is ``chain=True, K=2``.
"""
from __future__ import annotations

import re

from optv.shared.pdf2tei.config import ParliamentConfig
from optv.shared.lang.de import FACTIONS

CONFIG = ParliamentConfig(
    parliament_id="DE-BW",
    mp_speaker=re.compile(
        r"^Abg\.\s+(?P<name>[^:]+?)\s+"
        r"(?P<faction>GRÜNE|GRUNE|CDU|SPD|AfD|FDP/DVP|FDP)\s*:\s*(?P<rest>.*)$"),
    gov_speaker=re.compile(
        r"^(?P<role>Ministerpräsidentin|Ministerpräsident|Ministerin|Minister|"
        r"Staatssekretärin|Staatssekretär)(?:.*?)\s+"
        r"(?P<name>(?:Dr\.\s+|Prof\.\s+)*[A-ZÄÖÜ][a-zäöüß]+\s+"
        r"[A-ZÄÖÜ][a-zäöüß]+(?:-[A-ZÄÖÜ][a-zäöüß]+)?)\s*:\s*(?P<rest>.*)$"),
    chair_speaker=re.compile(
        r"^(?P<role>(?:Erste|Zweite|Dritte|Vierte)\s+Vizepräsidentin|"
        r"(?:Erster|Zweiter|Dritter|Vierter)\s+Vizepräsident|"
        r"Vizepräsidentin|Vizepräsident|Präsidentin|Präsident)\s+"
        r"(?P<name>[^:]+?)\s*:\s*(?P<rest>.*)$"),
    top_announce=None,  # TOC/heading-based agenda works better than chair announcements.
    known_factions=FACTIONS,
    speaker_inline=True,
    toc_noise=re.compile(
        r"^(Antrag|Antr[äa]ge|Alternativantrag|Gesetzentwurf|Beschluss|Drucksache|"
        r"Wahlvorschlag|Bericht der|Bericht und|Änderungsantrag|Entschließungsantrag|"
        r"Mündliche Anfrage|Beschlussempfehlung|Fragestunde|Befragung)\b",
        re.IGNORECASE),
    merge_chain=True,   # spine keeps the whole exchange in one clip
    merge_K=2,
)
