"""Parse the official per-clip text served by the IVOD site.

``https://ivod.ly.gov.tw/Demand/Speech/{IVOD_ID}`` (the ``speechRecordUrl`` of
open data dataset 421) returns a bare HTML fragment with the gazette record
of one clip: lines separated by ``<br />`` and hard-wrapped at the printed
line width, speaker turns introduced by a label such as ``陳委員昭姿：``
(surname + title + given name), ``卓院長榮泰：`` or ``主席：``. Names with a
Latin part (Indigenous legislators) put the title after the full name:
``伍麗華 Saidhai Tahovecahe 委員：``. One clip is a
dialogue: the legislator, the officials answering, and the chair. The page is
empty until the gazette is published, weeks after the sitting.

:func:`parse_speech_record` turns it into Stage 2 ``textBody`` items, one per
turn, with untimed sentences. A trailing written-interpellation stub
(``委員○○書面質詢：``) is not spoken and is dropped with everything after it.
"""

from __future__ import annotations

import html as _html
import re

_BR_RE = re.compile(r"<br\s*/?>", re.I)
_TAG_RE = re.compile(r"<[^>]+>")

# Titles that appear between surname and given name in turn labels. Longer
# alternatives first so e.g. 主任委員 wins over 委員.
_TITLES = (
    "主任委員", "召集委員", "政務委員", "委員長", "委員", "院長", "部長", "次長",
    "署長", "局長", "司長", "處長", "主委", "秘書長", "主計長", "審計長",
    "總長", "執行長", "董事長", "主任", "大法官", "檢察總長", "發言人", "代表",
)
_TITLE_ALT = r"副?(?:" + "|".join(_TITLES) + r")"
_LABEL_RE = re.compile(
    r"^(?P<surname>[\u4e00-\u9fff]{1,2}?)"
    r"(?P<title>" + _TITLE_ALT + r")"
    r"(?P<given>[\u4e00-\u9fff][\u4e00-\u9fffA-Za-z ·．]{0,19})：(?P<rest>.*)$"
)
# Full name with a Latin part, then the title.
_LATIN_NAME_LABEL_RE = re.compile(
    r"^(?P<name>[\u4e00-\u9fff]{1,4}\s*[A-Za-z][A-Za-z \-'’·．]*?)\s*"
    r"(?P<title>" + _TITLE_ALT + r")：(?P<rest>.*)$"
)
_CHAIR_RE = re.compile(r"^主席(?:（[^）]*）)?：(?P<rest>.*)$")
_CLOCK_RE = re.compile(r"^（\s*\d+\s*時(?:\s*\d+\s*分)?\s*）")
_WRITTEN_STUB_RE = re.compile(r"書面質詢：\s*$")
_SENTENCE_RE = re.compile(r".+?(?:[。！？!?]+[」』”’）)]*|$)", re.S)


def _norm(name: str) -> str:
    return re.sub(r"\s+", "", name or "")


def _lines(fragment: str) -> list[str]:
    out = []
    for raw in _BR_RE.split(fragment or ""):
        line = _html.unescape(_TAG_RE.sub("", raw)).strip()
        if line:
            out.append(line)
    return out


def _join(a: str, b: str) -> str:
    """Join a hard-wrapped continuation line. CJK text has no separator; only
    a Latin word broken across lines needs a space."""
    if a and b and a[-1].isascii() and a[-1].isalnum() and b[0].isascii() and b[0].isalnum():
        return a + " " + b
    return a + b


def split_sentences(text: str) -> list[str]:
    """Split after 。！？ (keeping the punctuation and closing quotes)."""
    return [s.strip() for s in _SENTENCE_RE.findall(text or "") if s.strip()]


def parse_speech_record(fragment: str, main_speaker: str = "") -> list[dict]:
    """Official per-clip HTML fragment → Stage 2 ``textBody`` items.

    ``speaker`` is the name without the title (``陳委員昭姿`` → ``陳昭姿``,
    ``伍麗華 Saidhai Tahovecahe 委員`` → ``伍麗華 Saidhai Tahovecahe``), ``主席`` for
    the chair. ``speakerstatus`` is ``president`` for the chair,
    ``main-speaker`` for the clip's legislator (``main_speaker``), else
    ``speaker``. Returns ``[]`` for an empty page.
    """
    main = _norm(main_speaker)
    turns: list[dict] = []
    for line in _lines(fragment):
        if _WRITTEN_STUB_RE.search(line):
            break
        chair = _CHAIR_RE.match(line)
        name, rest = None, None
        if not chair:
            label = _LABEL_RE.match(line)
            if label:
                name = (label.group("surname") + label.group("given")).strip()
                rest = label.group("rest")
            else:
                label = _LATIN_NAME_LABEL_RE.match(line)
                if label:
                    name, rest = label.group("name").strip(), label.group("rest")
        if chair:
            turns.append({"speaker": "主席", "speakerstatus": "president",
                          "text": chair.group("rest")})
        elif name:
            turns.append({"speaker": name,
                          "speakerstatus": "main-speaker" if _norm(name) == main else "speaker",
                          "text": rest})
        elif turns:
            turns[-1]["text"] = _join(turns[-1]["text"], line)
        else:
            turns.append({"speaker": None, "speakerstatus": None, "text": line})

    items: list[dict] = []
    for turn in turns:
        text = _CLOCK_RE.sub("", turn["text"].strip()).strip()
        sentences = [{"text": s} for s in split_sentences(text)]
        if not sentences:
            continue
        items.append({
            "type": "speech",
            "speaker": turn["speaker"],
            "speakerstatus": turn["speakerstatus"],
            "sentences": sentences,
        })
    return items
