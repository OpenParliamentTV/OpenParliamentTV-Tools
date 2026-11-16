"""Paragraph language identification for the Diario de Sesiones.

When a speaker uses Catalan (incl. Valencian), Galician or Basque, the Diario
prints each passage as spoken, immediately followed by its official Spanish
version, with no marker between the two. A speech may alternate: Spanish
passages appear once, each co-official passage is followed by its
translation. `split_language_passes` rebuilds the two renderings from
per-paragraph labels. The classifier counts function words that one
language uses and the others do not (the words Spanish shares with Catalan
or Galician, such as `el`, `la`, `que`, `por`, `con`, are left out), which
is enough for paragraphs of parliamentary prose.
"""

from __future__ import annotations

import math
import re

SPANISH = "es"

_WORDS = {
    "es": """
        y los las muy también hay gracias usted ustedes nosotros nosotras
        cuando más hacer ahora mucho muchos mucha muchas eso esto ellos ellas
        sus su lo le hoy ya pues fue decir otro otra otros otras estos esos
        sin donde entonces hemos vamos año años gobierno ley mismo misma
        solo hecho tiene tienen puede pueden primero nuestro nuestra
        nuestros nuestras quiero bien buenos buenas hablar hablado hablamos
        hasta siempre mientras luego después tampoco trabajo hacia aunque
        quien pueblo
    """,
    "ca": """
        i els amb per perquè què és molt molta molts moltes també aquest
        aquesta aquests aquestes això allò hi ho nosaltres vostè vostès vosté
        vostés seva seves seus gràcies senyor senyories doncs però quan més
        fer ara tots totes avui hem són ja dels pel pels als cal sense encara
        aquell aquella mateix mateixa uns unes nostre nostra nostres vostre
        vostra govern després abans sobretot fins mentre estan estem volem
        vull fet pot poden estes siga l d s n m
    """,
    "gl": """
        e os as do da das non unha unhas moi tamén grazas xa isto iso aquilo
        nós vostede vostedes polo pola polos polas coa coas cos na nas ao aos
        máis agora hoxe cando porén ten teñen facer dicir xente sen mais onde
        súa nosa noso nosos nosas hai foi fai pode deste desta neste nesta
        cun cunha dun dunha nun nunha ningún calquera mesmo goberno lei
        moito moita moitos moitas
    """,
    "eu": """
        eta ez bat ere dira dugu dute zen baina eskerrik asko gure zuen hori
        hau honek horrek behar egin ari izan dago daude baita gaur beste
        guztiak jauna andrea euskal herria herriak du ditu dituzte edo bere
        beren dela dutela duela zer nola zergatik hemen orain guk zuek haiek
        batzuk horiek hauek ezin bai ezta gero lehen arabera buruz ondoren
        bezala alde kontra legea gobernua gobernuak
    """,
}
_LEXICON = {lang: frozenset(words.split()) for lang, words in _WORDS.items()}
_TOKEN_RE = re.compile(r"[a-záéíóúàèìòùïüñç·]+")
# A paragraph needs this many marker words before it is labelled (one for a
# short paragraph such as "Moltes gràcies."), and the leading language must
# beat the runner-up by this factor.
_MIN_HITS = 3
_SHORT_TOKENS = 12
_MARGIN = 1.5
# Bounds on len(translation) / len(co-official passage). A Spanish version is
# about as long as its original (median 1.02 over the XV Diarios); a passage
# with no Spanish run of matching length after it is untranslated.
_RATIO_MIN = 0.6
_RATIO_MAX = 1.8


def paragraph_language(text: str):
    """Return 'es', 'ca', 'gl', 'eu' or None (too little evidence)."""
    tokens = _TOKEN_RE.findall(text.lower())
    hits = dict.fromkeys(_LEXICON, 0)
    for token in tokens:
        for lang, words in _LEXICON.items():
            if token in words:
                hits[lang] += 1
    ranked = sorted(hits.items(), key=lambda kv: kv[1], reverse=True)
    (top, top_hits), (_second, second_hits) = ranked[0], ranked[1]
    min_hits = 1 if len(tokens) < _SHORT_TOKENS else _MIN_HITS
    if top_hits < min_hits or top_hits < second_hits * _MARGIN:
        return None
    return top


def _runs(labels: list) -> list:
    """Group paragraph indices into (language, [index, ...]) runs.

    An unlabelled paragraph (a vocative such as "Señor ministro, buenas
    tardes.") opens the passage after it; trailing ones close the last run.
    """
    filled = list(labels)
    for i in range(len(filled) - 2, -1, -1):
        if filled[i] is None:
            filled[i] = filled[i + 1]
    for i in range(1, len(filled)):
        if filled[i] is None:
            filled[i] = filled[i - 1]
    runs: list = []
    for i, lang in enumerate(filled):
        if runs and runs[-1][0] == lang:
            runs[-1][1].append(i)
        else:
            runs.append((lang, [i]))
    return runs


def _translation(co_len: int, candidates: list, lengths: list):
    """The leading paragraphs of `candidates` whose length best matches a
    co-official passage of `co_len` characters, or None if none is close."""
    best, total = None, 0
    for j, i in enumerate(candidates):
        total += lengths[i]
        ratio = total / co_len
        if ratio > _RATIO_MAX:
            break
        if ratio >= _RATIO_MIN:
            score = abs(math.log(ratio))
            if best is None or score < best[0]:
                best = (score, j)
    return None if best is None else candidates[:best[1] + 1]


def _blockwise(paragraphs: list, labels: list):
    """(as_spoken, spanish, weight) pairing each co-official run with the
    Spanish run after it, or None if a run has no Spanish version."""
    lengths = [len(p) for p in paragraphs]
    runs = _runs(labels)
    as_spoken = set(range(len(paragraphs)))
    spanish = set(range(len(paragraphs)))
    weight: dict = {}
    for r, (lang, indices) in enumerate(runs):
        if lang == SPANISH:
            continue
        if r + 1 == len(runs) or runs[r + 1][0] != SPANISH:
            return None
        co_len = sum(lengths[i] for i in indices)
        translation = _translation(co_len, runs[r + 1][1], lengths)
        if translation is None:
            return None
        spanish.difference_update(indices)
        as_spoken.difference_update(translation)
        weight[lang] = weight.get(lang, 0) + co_len
    return sorted(as_spoken), sorted(spanish), weight


def _whole_turn(paragraphs: list, labels: list):
    """(as_spoken, spanish, weight) for a turn printed as the full original
    followed by the full Spanish version: the Spanish version is everything
    after the last co-official paragraph. The original may itself contain
    Spanish (quotes, passages spoken in Spanish). None if the two are not of
    matching length."""
    co = [i for i, lang in enumerate(labels) if lang not in (None, SPANISH)]
    k = co[-1] + 1
    original, version = range(k), range(k, len(paragraphs))
    if not any(labels[i] == SPANISH for i in version):
        return None
    ratio = sum(len(paragraphs[i]) for i in version) / sum(len(paragraphs[i]) for i in original)
    if not _RATIO_MIN <= ratio <= _RATIO_MAX:
        return None
    weight: dict = {}
    for i in co:
        weight[labels[i]] = weight.get(labels[i], 0) + len(paragraphs[i])
    return list(original), list(version), weight


def split_language_passes(paragraphs: list):
    """Split a turn's paragraphs into (language, as_spoken, spanish).

    `as_spoken` drops the Spanish translations, `spanish` drops the
    co-official passages; passages spoken in Spanish are in both when the
    Diario prints them once. Tries the passage-by-passage layout first, then
    the full-original-then-full-translation one. The language is the
    co-official language with the most text. Returns None for a turn that is
    all Spanish, or whose co-official text has no Spanish version (the Diario
    then carries only the text as spoken).
    """
    labels = [paragraph_language(p) for p in paragraphs]
    if all(lang in (None, SPANISH) for lang in labels):
        return None
    split = _blockwise(paragraphs, labels) or _whole_turn(paragraphs, labels)
    if split is None:
        return None
    as_spoken, spanish, weight = split
    return (max(weight, key=weight.get),
            [paragraphs[i] for i in as_spoken],
            [paragraphs[i] for i in spanish])
