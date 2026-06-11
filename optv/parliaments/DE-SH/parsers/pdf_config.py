"""DE-SH (Schleswig-Holsteinischer Landtag) PDF→TEI detection config (lifted from prototype).

SH labels appear both as their own block ("Name [CDU]:") and inline; the spine
is clean, so the rede-merge is ``chain=False``.
"""
from __future__ import annotations

import re

from optv.shared.pdf2tei.config import ParliamentConfig
from optv.shared.lang.de import FACTIONS

CONFIG = ParliamentConfig(
    parliament_id="DE-SH",
    mp_speaker=re.compile(
        r"^(?P<name>[A-ZÄÖÜ][^\[\]]+?)\s*\[(?P<faction>[^\]]+)\]\s*:\s*(?P<rest>.*)$"),
    gov_speaker=re.compile(
        r"^(?P<name>[A-ZÄÖÜ][^,\[\]]+?)\s*,\s+(?P<role>(?:Minister(?:in)?|Staatssekretär(?:in)?|"
        r"Ministerpräsident(?:in)?)[^:]*?)\s*:\s*(?P<rest>.*)$"),
    chair_speaker=re.compile(
        r"^(?P<role>Vizepräsidentin|Vizepräsident|Alterspräsidentin|Alterspräsident|"
        r"Präsidentin|Präsident)\s+(?P<name>[^:]+?)\s*:\s*(?P<rest>.*)$"),
    top_announce=None,  # SH rarely announces "Ich rufe TOP N"; rely on TOC.
    known_factions=FACTIONS,
    speaker_inline=True,
    toc_noise=re.compile(
        r"^(Antrag|Antr[äa]ge|Alternativantrag|Gesetzentwurf|Beschluss|Drucksache|"
        r"Wahlvorschlag|Bericht der|Bericht und|Änderungsantrag|Entschließungsantrag|"
        r"Mündliche Anfrage|Beschlussempfehlung)\b", re.IGNORECASE),
    merge_chain=False,
)
