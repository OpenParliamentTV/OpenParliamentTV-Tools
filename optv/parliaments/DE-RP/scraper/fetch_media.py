#! /usr/bin/env python3

# Convert OPAL "Suche nach Reden" rendered-search-result HTML pages into
# raw per-session media JSON files for the parser to consume.
#
# OPAL is a JS SPA wrapping Cuadra STAR; result pages must be rendered by a
# browser (saved manually, or by `fetch_opal.py` for test runs). This
# scraper expects HTML files dropped into <data>/original/media/inbox/.
#
# Per row we extract speaker name + party/office, page range, function tag
# and the direct .mp4 URL (data-video on the play button). The electoral
# period comes from the <p>Plenarsitzung WP/SS</p> header that precedes the
# rows of each sitting: the video file names carry no period.
#
# Video files are named `{sitting}-Sit{N}{suffix}.mp4`. N is a running
# number in protocol order; a clip that follows N without a number of its
# own (some Kurzinterventionen and replies, statements of the presiding
# officer, occasionally a speech) gets a letter suffix `a`-`d`. Rows sort
# by (N, suffix). The server reuses `{sitting}-Sit{N}.mp4` in the next
# electoral period, so `--fingerprint-media` records Last-Modified and
# Content-Length per clip and `--recheck-media` detects a replaced file.
#
# Output: <data>/original/media/raw-<session>-media.json
# Re-running merges new HTML inputs into existing per-session files.

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import logging
from lxml import html as lxml_html
from pathlib import Path
import re
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

VIDEO_HOST = "https://dokumente.landtag.rlp.de"
OPAL_BASE = "https://opal.rlp.de/"
# OPAL's per-speech HTML player. The `videourl` query param is the unique
# per-speech key — using this URL as `sourcePage` gives the platform a
# deep link that's one-to-one with the speech (within an electoral period).
OPAL_PLAYER = "https://opal.rlp.de/cgi-bin/ff.pl?form=video.html&videourl="

VIDEO_URL_RE = re.compile(
    r"^https?://[^/]+/landtag/opal-videos/"
    r"(?P<stem>(?P<session>\d+)-Sit(?P<index>\d+)(?P<suffix>[a-z]?))\.mp4$"
)

# A row's text looks like one of:
#   Wansch, Thomas  (SPD) S. 26-30
#   Wansch, Thomas  (SPD) S. (Bericht) 26-30
#   Schnieder, Gordon  (CDU) S. 63-65 (Kurzintervention)
#   Maier, Florian (SPD) S. 52-53(Kurzintervention)
#   Schneid, Marion (Landtagsvizepräsidentin) Bekanntgabe des Abstimmungsergebnisses: S. 154
#   Binz, Katharina (BÜNDNIS 90/DIE GRÜNEN)
#   Roland Weimer (CDU) S. 120-122
# The parenthesised value is a faction or an office (chair, ministry code,
# Ministerpräsident, …); the parser tells them apart.
ROW_RE = re.compile(
    r"^(?P<name>[^()]+?)\s*\((?P<party>[^)]+)\)\s*"
    r"(?P<func0>[^()]*?:)?\s*"
    r"(?:S\.?\s*(?:\((?P<func1>[^)]+)\)\s*)?(?P<pages>[\d-]+)?\s*)?"
    r"(?:\((?P<func2>[^)]+)\))?\s*$",
    re.UNICODE,
)

# Row fields that describe the video file rather than the OPAL listing; they
# survive a re-ingest of the same row.
FINGERPRINT_FIELDS = ("video_last_modified", "video_content_length",
                      "video_missing", "video_replaced")

USER_AGENT = "OpenParliamentTV-Tools (+https://github.com/OpenParliamentTV)"


def parse_session_header(text: str) -> tuple[str, str] | None:
    """Match 'Plenarsitzung 18/77' → ('18','77')."""
    m = re.match(r"\s*Plenarsitzung\s+(\d+)/(\d+)\s*$", text or "")
    if not m:
        return None
    return m.group(1), m.group(2)


def normalize_speaker(label: str) -> tuple[str, str]:
    """Return (firstname, lastname).

    'Wansch, Thomas' → ('Thomas', 'Wansch'); OPAL occasionally lists a name
    in reading order: 'Roland Weimer' → ('Roland', 'Weimer').
    """
    label = re.sub(r"\s+", " ", label).strip()
    parts = [p.strip() for p in label.split(",", 1)]
    if len(parts) == 2:
        return parts[1], parts[0]
    first, _, last = label.rpartition(" ")
    if not first:
        return label, ""
    return first, last


def row_order(row: dict) -> tuple[int, str]:
    """Protocol order: N, Na, Nb, …, N+1."""
    return row["speech_index_in_session"], row.get("speech_suffix", "")


def _is_detail_row(p_el) -> bool:
    classes = (p_el.get("class") or "").split()
    return "ml-5" in classes and "border-left" in classes


def _row_text_excluding_anchor(p_el, anchor) -> str:
    """Return the text inside <p>, excluding the play-button <a>'s subtree."""
    parts: list[str] = []
    if p_el.text:
        parts.append(p_el.text)
    for child in p_el:
        if child is anchor:
            if child.tail:
                parts.append(child.tail)
            continue
        if anchor in child.iter():
            # Skip the anchor's subtree but keep tail text after it.
            for sub in child.iter():
                if sub is anchor:
                    if sub.tail:
                        parts.append(sub.tail)
                    continue
                if sub is not child and sub.text:
                    parts.append(sub.text)
                if sub is not child and sub.tail and sub is not anchor:
                    parts.append(sub.tail)
            if child.tail:
                parts.append(child.tail)
            continue
        # Whole subtree text
        for sub in child.iter():
            if sub is not child and sub.text:
                parts.append(sub.text)
            if sub.tail:
                parts.append(sub.tail)
        if child.text:
            parts.append(child.text)
        if child.tail:
            parts.append(child.tail)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def extract_rows(doc) -> list[dict]:
    """Walk the speaker-detail rows in document order; emit one dict per row.

    OPAL renders each speech several times (tile summary plus expandable
    speaker-detail rows under each sitting). We target the speaker-detail
    rows (<p class="ml-5 ... border-left ..."> with a data-video anchor),
    the only complete rows. Each sitting's rows follow a
    <p>Plenarsitzung WP/SS</p> header, which supplies the electoral period.
    """
    root = doc.getroot() if hasattr(doc, "getroot") else doc
    rows_seen: dict[str, dict] = {}
    problems: set[str] = set()
    current: tuple[str, str] | None = None

    for p_el in root.iter("p"):
        if not _is_detail_row(p_el):
            header = parse_session_header(p_el.text_content())
            if header:
                current = header
            continue
        anchors = p_el.xpath(".//a[@data-video]")
        if not anchors:
            continue
        a = anchors[0]
        video_url = a.attrib.get("data-video") or ""
        vm = VIDEO_URL_RE.match(video_url)
        if not vm:
            if video_url not in problems:
                problems.add(video_url)
                logger.warning(f"Skipping non-OPAL video URL: {video_url}")
            continue
        if current is None:
            if video_url not in problems:
                problems.add(video_url)
                logger.warning(f"Skipping row before any 'Plenarsitzung' header: {video_url}")
            continue
        period, header_sitting = current
        if int(header_sitting) != int(vm.group("session")):
            if video_url not in problems:
                problems.add(video_url)
                logger.warning(f"Skipping {video_url}: listed under 'Plenarsitzung "
                               f"{period}/{header_sitting}'")
            continue

        origin_media_id = f"{period}-{vm.group('stem')}"
        if origin_media_id in rows_seen or origin_media_id in problems:
            # OPAL repeats each row; keep the first.
            continue
        text = _row_text_excluding_anchor(p_el, a)
        m = ROW_RE.match(text)
        if not m:
            problems.add(origin_media_id)
            logger.warning(f"Skipping unparseable row {origin_media_id}: {text!r}")
            continue
        firstname, lastname = normalize_speaker(m.group("name"))
        function = (m.group("func0") or m.group("func1") or m.group("func2") or "")
        function = function.strip().rstrip(":").strip()

        rows_seen[origin_media_id] = {
            "parliament": "DE-RP",
            "session_period": period,
            "session_number": vm.group("session"),
            "speech_index_in_session": int(vm.group("index")),
            "speech_suffix": vm.group("suffix"),
            "speaker_label": f"{firstname} {lastname}".strip(),
            "speaker_firstname": firstname,
            "speaker_lastname": lastname,
            # OPAL's parenthesised value: a faction or an office.
            "faction": (m.group("party") or "").strip(),
            "page_range": (m.group("pages") or "").strip(),
            "function": function,
            "video_url": video_url,
            "origin_media_id": origin_media_id,
            "source_page": f"{OPAL_PLAYER}{video_url}",
        }
    skipped = len(problems)
    if skipped:
        logger.warning(f"Skipped {skipped} row(s); see warnings above")
    return list(rows_seen.values())


def parse_html_file(path: Path) -> list[dict]:
    # OPAL serves UTF-8; don't rely on a saved page keeping its <meta charset>.
    with path.open("rb") as f:
        doc = lxml_html.parse(f, parser=lxml_html.HTMLParser(encoding="utf-8"))
    rows = extract_rows(doc)
    logger.info(f"{path.name}: parsed {len(rows)} rows")
    return rows


def _normalize_row(row: dict) -> dict:
    """Bring a raw row to the current shape (period-qualified id, suffix).

    Rows written before the id carried the period (`77-Sit62`) are re-keyed
    from their video URL so they dedupe against freshly parsed rows.
    """
    vm = VIDEO_URL_RE.match(row.get("video_url") or "")
    if vm:
        row["origin_media_id"] = f"{row['session_period']}-{vm.group('stem')}"
        row["speech_suffix"] = vm.group("suffix")
        row["speech_index_in_session"] = int(vm.group("index"))
    row.setdefault("speech_suffix", "")
    return row


def _raw_session_id(row: dict) -> str:
    return f"{row['session_period']}{row['session_number'].zfill(3)}"


def _write_raw(path: Path, sid: str, rows: list[dict]) -> None:
    out = {
        "meta": {
            "session": sid,
            "source": "opal-search-rendered-html",
            "ingested_at": datetime.now().isoformat("T", "seconds"),
        },
        "rows": rows,
    }
    with path.open("w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)


def merge_into_raw_files(rows: list[dict], media_dir: Path) -> dict[str, int]:
    """Append rows to per-session raw-<session>-media.json files.

    Existing rows are deduped by origin_media_id so the same HTML can be
    re-ingested without duplication; a re-ingested row keeps the fingerprint
    of the row it replaces.
    """
    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)

    by_session: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_session[_raw_session_id(r)].append(r)

    counts: dict[str, int] = {}
    for sid, new_rows in by_session.items():
        path = media_dir / f"raw-{sid}-media.json"
        existing: list[dict] = []
        if path.exists():
            with path.open() as f:
                existing = json.load(f).get("rows", [])
        index = {}
        for r in existing:
            r = _normalize_row(r)
            index[r["origin_media_id"]] = r
        for r in new_rows:
            old = index.get(r["origin_media_id"])
            if old:
                for field in FINGERPRINT_FIELDS:
                    if field in old and field not in r:
                        r[field] = old[field]
            index[r["origin_media_id"]] = r
        merged = sorted(index.values(), key=row_order)
        _write_raw(path, sid, merged)
        counts[sid] = len(merged)
    return counts


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def head_request(url: str, timeout: float = 30) -> tuple[int, dict]:
    """HEAD without following redirects; return (status, headers).

    A missing file redirects (302) to the Landtag's start page.
    """
    opener = urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {})


def _fingerprint(headers: dict) -> dict:
    headers = {k.lower(): v for k, v in headers.items()}
    fp = {}
    if headers.get("last-modified"):
        try:
            lm = parsedate_to_datetime(headers["last-modified"])
            fp["video_last_modified"] = lm.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            logger.warning(f"Unparseable Last-Modified: {headers['last-modified']!r}")
    if headers.get("content-length", "").isdigit():
        fp["video_content_length"] = int(headers["content-length"])
    return fp


class Throttle:
    """At most one call per `delay` seconds."""

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self._last = None

    def __call__(self):
        if self._last is not None and self.delay > 0:
            wait = self.delay - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
        self._last = time.monotonic()


def fingerprint_rows(rows: list[dict], recheck: bool = False,
                     head=head_request, throttle=None) -> int:
    """Record Last-Modified/Content-Length of each row's video file.

    Without `recheck`, only rows that have no fingerprint yet are requested.
    With `recheck`, every row is requested and a file whose Last-Modified or
    Content-Length differs from the recorded one is marked `video_replaced`
    (the server reuses file names in the next electoral period). A redirect
    marks `video_missing`. Returns the number of rows changed.
    """
    throttle = throttle or Throttle()
    changed = 0
    for row in rows:
        known = "video_last_modified" in row or row.get("video_missing")
        if known and not recheck:
            continue
        throttle()
        try:
            status, headers = head(row["video_url"])
        except (urllib.error.URLError, OSError) as e:
            logger.warning(f"HEAD {row['video_url']} failed: {e}")
            continue
        if status in (301, 302, 303, 307, 308, 404):
            if not row.get("video_missing"):
                logger.error(f"{row['origin_media_id']}: video file is gone "
                             f"({status}): {row['video_url']}")
                row["video_missing"] = True
                changed += 1
            continue
        if status != 200:
            logger.warning(f"HEAD {row['video_url']}: HTTP {status}")
            continue
        fp = _fingerprint(headers)
        if not fp:
            continue
        if "video_last_modified" not in row and "video_content_length" not in row:
            row.update(fp)
            changed += 1
            continue
        differs = any(row.get(k) != v for k, v in fp.items())
        if differs and not row.get("video_replaced"):
            logger.error(f"{row['origin_media_id']}: video file was replaced "
                         f"(recorded {row.get('video_last_modified')}, "
                         f"{row.get('video_content_length')} bytes; now "
                         f"{fp.get('video_last_modified')}, "
                         f"{fp.get('video_content_length')} bytes): {row['video_url']}")
            row["video_replaced"] = True
            changed += 1
    return changed


def fingerprint_media_dir(media_dir: Path, recheck: bool = False,
                          head=head_request, delay: float = 1.0) -> int:
    """Fingerprint the rows of every raw-*-media.json file in media_dir."""
    throttle = Throttle(delay)
    total = 0
    for path in sorted(Path(media_dir).glob("raw-*-media.json")):
        raw = json.loads(path.read_text())
        rows = [_normalize_row(r) for r in raw.get("rows", [])]
        changed = fingerprint_rows(rows, recheck=recheck, head=head, throttle=throttle)
        if changed:
            _write_raw(path, raw["meta"]["session"], rows)
            logger.info(f"{path.name}: updated fingerprints of {changed} row(s)")
        total += changed
    return total


def ingest_html_inbox(inbox_dir: Path, media_dir: Path, fingerprint: bool = False,
                      recheck: bool = False) -> dict[str, int]:
    inbox_dir = Path(inbox_dir)
    counts: dict[str, int] = {}
    if inbox_dir.is_dir():
        all_rows: list[dict] = []
        for src in sorted(inbox_dir.glob("*.html")):
            all_rows.extend(parse_html_file(src))
        counts = merge_into_raw_files(all_rows, media_dir)
    else:
        logger.warning(f"Media inbox does not exist: {inbox_dir}")
    if fingerprint or recheck:
        fingerprint_media_dir(media_dir, recheck=recheck)
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert saved OPAL HTML pages into raw media JSON.")
    parser.add_argument("--inbox-dir", required=True,
                        help="Directory containing saved OPAL search HTML pages")
    parser.add_argument("--media-dir", required=True,
                        help="Target directory (typically <data>/original/media)")
    parser.add_argument("--fingerprint-media", action="store_true", default=False,
                        help="HEAD each new video file (1 request/s) and record "
                             "Last-Modified and Content-Length")
    parser.add_argument("--recheck-media", action="store_true", default=False,
                        help="HEAD every video file again and flag replaced files")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    counts = ingest_html_inbox(Path(args.inbox_dir), Path(args.media_dir),
                               fingerprint=args.fingerprint_media,
                               recheck=args.recheck_media)
    for sid, n in sorted(counts.items()):
        logger.info(f"  {sid}: {n} rows")
    logger.info("Done")
