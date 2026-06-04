"""Language-specific text helpers, keyed by ISO 639-1 code.

Each module (``de``, ``es``, …) holds conventions that apply to **all
parliaments speaking that language**, not to a single parliament: honorific
stripping, chair-title → speaker ``context`` mapping, and similar. A German
helper in :mod:`optv.shared.lang.de` is shared by DE, DE-RP, DE-ST and the
video-only Landtage alike; a future :mod:`optv.shared.lang.es` would serve the
Spanish-speaking parliaments.

This is distinct from the manifest's ``locale`` block (spaCy / aeneas /
entity-fishing config) — ``lang`` is *code*, ``locale`` is *config*.
"""
