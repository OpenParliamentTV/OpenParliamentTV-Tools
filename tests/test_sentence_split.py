"""Length-gated secondary sentence splitting (optv/shared/sentence_split.py)."""

from optv.shared.sentence_split import (
    split_long_sentence, split_long_sentences,
)


def wc(s):
    return len(s.split())


def test_short_sentence_is_untouched():
    s = "Das ist ein kurzer Satz."
    assert split_long_sentence(s, threshold=50) == [s]


def test_at_threshold_is_untouched():
    s = " ".join(["wort"] * 50) + "."
    assert split_long_sentence(s, threshold=50) == [s]


def test_splits_on_semicolon_first():
    a = " ".join(["aaa"] * 30)
    b = " ".join(["bbb"] * 30)
    out = split_long_sentence(f"{a}; {b}.", threshold=50)
    assert len(out) == 2
    assert out[0].startswith("aaa") and out[1].startswith("bbb")


def test_splits_on_colon_when_no_semicolon():
    head = " ".join(["x"] * 30)
    tail = " ".join(["y"] * 30)
    out = split_long_sentence(f"{head}: {tail}.", threshold=50)
    assert len(out) == 2


def test_paired_dash_interjection_kept_in_order():
    head = " ".join(["h"] * 30)
    aside = " ".join(["a"] * 10)
    tail = " ".join(["t"] * 30)
    out = split_long_sentence(f"{head} – {aside} – {tail}.", threshold=50)
    # spoken order: head, aside, tail -- each its own piece
    assert [p[0] for p in out] == ["h", "a", "t"]


def test_tiny_aside_not_left_as_standalone_piece():
    head = " ".join(["h"] * 40)
    tail = " ".join(["t"] * 20)
    # 2-word aside is below min_words -> rides along with an adjacent clause,
    # never its own unit
    out = split_long_sentence(f"{head} – kurzer Einschub – {tail}.",
                              threshold=50, min_words=4)
    assert all(wc(p) >= 4 for p in out)
    assert any("kurzer Einschub" in p for p in out)


def test_comma_split_only_above_hard_cap():
    # 60-word comma enumeration, no ;/:/dash: untouched at hard_cap 80 ...
    enum = ", ".join(["punkt"] * 60) + "."
    assert split_long_sentence(enum, threshold=50, hard_cap=80) == [enum]
    # ... but split once it exceeds the hard cap
    big = ", ".join(["punkt"] * 90) + "."
    assert len(split_long_sentence(big, threshold=50, hard_cap=80)) > 1


def test_compound_hyphen_is_not_a_split_point():
    # "Strom- und Wärmemarkt": hyphen has no surrounding spaces -> never split
    s = ("Wir greifen in den Strom- und Wärmemarkt ein " + " ".join(["x"] * 50)
         + ".")
    out = split_long_sentence(s, threshold=50)
    assert all("Strom-" not in p or "Wärmemarkt" in p for p in out)


def test_recurses_until_pieces_fit():
    a = " ".join(["a"] * 30)
    b = " ".join(["b"] * 30)
    c = " ".join(["c"] * 30)
    out = split_long_sentence(f"{a}; {b}: {c}.", threshold=50)
    assert len(out) == 3
    assert all(wc(p) <= 50 for p in out)


def _content(s):
    # words with split-delimiter punctuation stripped (the breaker drops the
    # standalone ; : – tokens it cuts on, but never a content word)
    import re
    return [w for w in re.sub(r"[;:–—]", " ", s).split()]


def test_concatenation_preserves_content_words():
    s = ("Erstens dies und das und jenes; zweitens etwas ganz anderes – ein "
         "kurzer Hinweis – und noch viel mehr dazu: nämlich diese lange "
         "Aufzählung mit vielen Wörtern darin " + " ".join(["wort"] * 40) + ".")
    out = split_long_sentence(s, threshold=50)
    assert _content(" ".join(out)) == _content(s)


def test_split_long_sentences_flattens():
    short = "Kurzer Satz."
    long = " ".join(["a"] * 30) + "; " + " ".join(["b"] * 30) + "."
    out = split_long_sentences([short, long], threshold=50)
    assert out[0] == short
    assert len(out) == 3


# German conjunction-aware splitting (optv/shared/lang/de.py wrapper).


def test_german_splits_conditional_chain_at_clauses():
    from optv.shared.lang.de import split_long_sentence as de_split
    s = ("Wenn uns 17 Milliarden Euro fuer wichtige Investitionen wegfallen, "
         "wenn wir immer noch Krisenauswirkungen von Corona spueren mussten, "
         "wenn wir zudem eine schwere Energiekrise zu bewaeltigen haben, dann "
         "ist es wahrlich nicht leicht, diese Aufgaben alle zu bewaeltigen.")
    out = de_split(s, threshold=20)
    assert len(out) >= 4
    assert out[1].startswith("wenn")
    assert out[-1].startswith("dann")


def test_german_splits_dass_chain():
    from optv.shared.lang.de import split_long_sentence as de_split
    s = ("Ich wies darauf hin, dass wir die Fallpauschalen ueberwinden wollen, "
         "dass wir die Geburtshilfe im laendlichen Bereich anders darstellen "
         "wollen, dass wir die stationaere Versorgung grundlegend verbessern, "
         "sodass die ambulante Leistungserbringung kuenftig moeglich wird.")
    out = de_split(s, threshold=20)
    assert any(p.startswith("dass wir die Geburtshilfe") for p in out)
    assert all(wc(p) <= 20 for p in out)


def test_german_short_sentence_untouched():
    from optv.shared.lang.de import split_long_sentence as de_split
    s = "Ich danke Ihnen, dass Sie gekommen sind."
    assert de_split(s, threshold=50) == [s]


def test_german_does_not_split_bare_article_comma():
    # ", die"/", der" bare relatives are excluded -> an enumeration with article
    # commas isn't shredded on every comma (only a real >hard_cap fallback).
    from optv.shared.lang.de import split_long_sentence as de_split
    s = ("Ich danke der Kollegin, der Beamtin und dem Helfer " +
         " ".join(["sehr"] * 45) + ".")
    out = de_split(s, threshold=50, hard_cap=80)
    # 52 words, no ;/:/dash, no clause-starter, under hard cap -> untouched
    assert out == [s]
