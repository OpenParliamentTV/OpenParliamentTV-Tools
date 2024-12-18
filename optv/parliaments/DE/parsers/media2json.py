#! /usr/bin/env python3

# Convert RSS media index file from http://webtv.bundestag.de into JSON with
# fields defined for OpenParliamentTV

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime, timedelta, timezone
import feedparser
import json
import os
from pathlib import Path
import re
import sys
from typing import Optional
from urllib.parse import urlparse, parse_qs
import yaml

try:
    from parsers.common import fix_faction, fix_fullname, fix_role, fixup_execute
except ModuleNotFoundError:
    # Module not found. Tweak the sys.path
    base_dir = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(base_dir))
    from parsers.common import fix_faction, fix_fullname, fix_role

# Constants used for basic integrity checking: If these values are not
# present in the source data, then something must have changed and the
# parser should be checked anyway.
FEED_SUBTITLE = 'Deutscher Bundestag'
FEED_LICENSE = '&lt;a href=&quot;https://www.bundestag.de/nutzungsbedingungen&quot; target=&quot;_blank&quot;&gt;Nutzungsbedingungen&lt;/a&gt;'
FEED_AUTHOR_EMAIL = 'mail@bundestag.de'
# Note that <faction> may be empty (in the case of Nationalhymne)
title_data_re = re.compile(r'Redebeitrag\s+von\s+(?P<fullname>.+?)\s+\((?P<faction>.*?)\),?\s+am (?P<title_date>[\d.]+)\s+um\s+(?P<title_time>[\d:]+)\s+Uhr\s+\((?P<session_info>.+)\)')

def extract_title_data(title: str) -> Optional[dict]:
    """Extract structured data from title string.

    Return a dict with fields if data could be extracted, else None.
    """
    # "Redebeitrag von Stephan Stracke (CDU/CSU) am 29.01.2010 um 14:05 Uhr (20. Sitzung, TOP ZP 2)"
    match = title_data_re.match(title)
    if match:
        return match.groupdict()
    else:
        return None

def fix_title(title: str) -> str:
    """Fix the titles to match with proceedings conventions
    """
    title = title.replace("TOP Sitzungsende", "Sitzungsende").replace("TOP Sitzungseröffnung", "Sitzungseröffnung")

    zusatz = re.findall(r'TOP(?:\s+\d+)?,?\s+(ZP|Epl)\s*(\d+)', title, flags=re.IGNORECASE)
    if zusatz:
        if zusatz[0][0].lower() == 'zp':
            title = f"Zusatzpunkt {zusatz[0][1]}"
        else:
            title = f"Einzelplan {zusatz[0][1]}"
    title = re.sub(r'^TOP\s+(.+)', 'Tagesordnungspunkt \\1', title)
    title = title.rstrip(".")
    return title

def apply_media_fixups(entry: dict, meeting_reference: int, fixups: dict):
    # Apply fixups to a given entry

    # Are there any fixups valid for this meeting's media? This should be a list of actions
    media_fixups = fixups.get(meeting_reference, {}).get('media', [])
    for fix in media_fixups:
        entry = fixup_execute(fix, entry)
    return entry

def parse_media_data(data: dict, fixups: dict = None) -> dict:
    """Parse a media-js structure

    It is a dict with
    {
    'root': root_feed_object,
    'entries': list_of_entries_to_parse
    }

    This generic structure is meant to accomodate single XML dumps of
    RSS feeds (in which case root.entries == entries) and the output
    of fetch_media script (which aggregates multiple pages of items
    into entries).

    fixups is a dict of hardcoded fixups for wrong data.
    It is indexed first by session number (20023) then by media/proceeding

    Returns a list of dict each holding information about an item.
    """
    if fixups is None:
        fixups = {}
    output: list[dict] = []
    root = data['root']
    entries = data['entries']

    # Do some validity checks
    if root['feed'].get('subtitle') != FEED_SUBTITLE:
        logger.error(f"Feed subtitle is not {FEED_SUBTITLE}: {root['feed'].get('subtitle')}")
        return output
    if root['feed']['author_detail'].get('email') != FEED_AUTHOR_EMAIL:
        logger.error(f"Feed author is not {FEED_AUTHOR_EMAIL}: {root['feed']['author_detail'].get('email')}")
        return output

    # Convert links list to dict indexed by 'rel'
    session_links = dict( (link['rel'], link) for link in root['feed']['links'] )
    if not session_links.get('self'):
        logger.error("No session information")
        return output
    session_href = session_links.get('self')['href']

    # Parse session_href URI to get period and meeting number
    # 'http://webtv.bundestag.de/player/macros/_v_q_2192_de/_s_podcast_skin/_x_s-144277506/bttv/podcast.xml?period=19&meetingNumber=4',
    session_info = parse_qs(urlparse(session_href).query)
    if not session_info.get('period'):
        logger.error("No period number")
        return output
    if not session_info.get('meetingNumber'):
        logger.error("No meeting number")
        return output
    period_number = int(session_info['period'][0])
    meeting_number = int(session_info['meetingNumber'][0])
    meeting_reference = 1000 * period_number + meeting_number

    for e in entries:
        e = apply_media_fixups(e, meeting_reference, fixups)
        links = dict( (link['rel'], link) for link in e['links'] )

        if 'enclosure' not in links:
            # No media associated to the item.
            # FIXME: should we report the issue?
            logger.debug(f"No media associated: {e['title']}")
            continue

        # Use duration to compute end time
        t = datetime.strptime(e['itunes_duration'],"%H:%M:%S")
        delta = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)

        # FIXME: el['published_parsed'] in in UTC.  But TZ info is
        # present in el['published'] and the proceedings official
        # hours (sitzung-start-uhrzeit/sitzung-ende-uhrzeit) are in
        # local time. So get the UTC offset from media, and we will
        # add this information to the proceedings one.
        startdate = datetime(*e['published_parsed'][:6])

        # Timezone info is not preserved by feedparser, re-add it:
        # Get UTC offset from el['published']
        utc_offset = e['published'].strip()[-5:]
        m = re.match(r'^([+-])(\d\d)(\d\d)$', utc_offset)
        if m:
            sign, hours, minutes = m.groups()
            # Found a valid UTC offset - we should make the startdate
            # aware
            utc_delta = timedelta(hours=int(f"{sign}{hours}"),
                                  minutes=int(f"{sign}{minutes}"))
            tz = timezone(utc_delta)
            # We add the utc_delta to naive startdate, so that it
            # corresponds to the tzinfo we replace after.
            startdate = startdate + utc_delta
            startdate = startdate.replace(tzinfo=tz)

        enddate = startdate + delta
        mediaid = os.path.basename(e['link'])

        item: dict = {
            "parliament": "DE",
            "electoralPeriod": {
                "number": period_number,
            },
            "session": {
                "number": meeting_number,
            },
            "agendaItem": {
                'title': e.get('subtitle'),
                # Note: this will get replaced below by the result of extract_title_data + sub + fix_title
                'officialTitle': fix_title(e['title']),
            },
            'dateStart': startdate.isoformat('T', 'seconds'),
            'dateEnd': enddate.isoformat('T', 'seconds'),
            'debug': {
                'originalTitle': e['title']
            },
            "media": {
                'videoFileURI': links['enclosure']['href'],
                'sourcePage': e['link'],
                'duration': delta.total_seconds(),
                'creator': e['author'],

                # Note: commented fields are defined in
                # https://github.com/OpenParliamentTV/OpenParliamentTV-Platform/issues/2
                # but not available here

                #'audioFileURI': '' ,
                #"thumbnailURI": "https://example.com/thumb.png",
                #"thumbnailCreator": "Deutscher Bundestag",
                #"thumbnailLicense": "CC-BY-SA",
                "license": FEED_LICENSE,
                "originMediaID": mediaid,
                # "sourcePage": "https://dbtg.tv/fvid/7502148"
                # 'sourceFilename': filename,
            },
        }
        if period_number >= 18:
            # item['media']['audioFileURI'] = f"""https://static.p.core.cdn.streamfarm.net/1000153copo/ondemand/145293313/{mediaid}/{mediaid}_mp3_128kb_stereo_de_128.mp3"""
            item['media']['audioFileURI'] = f"""https://cldf-od.r53.cdn.tv1.eu/1000153copo/ondemand/app144277506/145293313/{mediaid}/{mediaid}_mp3_128kb_stereo_de_128.mp3"""

        metadata = extract_title_data(e['title'])
        if metadata is not None:
            # Faction may encode only faction, or role/faction information.
            # jq -r '.[] | .people[0].faction' data/examples/nmedia/*json | sort -u
            # in old dumps to get all different values.
            full_faction = metadata.get('faction', '')
            if '/' in full_faction:
                # Maybe it encodes a role
                role, faction = full_faction.split('/', 1)
                if role in ('CDU', 'B90', 'Bündnis 90'):
                    # Special cases for CDU and B90
                    faction = full_faction
                    role = None
            else:
                faction = full_faction
                role = None
            person = {
                'label': fix_fullname(metadata.get('fullname', '')),
                'context': 'main-speaker',
            }
            if role is not None:
                person['role'] = fix_role(role)
            if faction:
                person['faction'] = fix_faction(faction)
            item['people'] = [ person ]
            if metadata.get('session_info') is not None:
                # According to https://github.com/OpenParliamentTV/OpenParliamentTV-Parsers/issues/1
                # we should strip the Sitzung prefix from the session_info
                item['agendaItem']['officialTitle'] = fix_title(re.sub(r'^\d+\.\sSitzung,\s', '', metadata.get('session_info')))
            # FIXME: we have other fields: title_date, title_time that we could use for validation

        # Fix AgendaItemTitle if necessary
        if not item['agendaItem']['title']:
            title = fix_title(item['agendaItem']['officialTitle'])
            item['agendaItem']['title'] = title

        output.append(item)

    # Sort output by startDate - we have it here in ISO format so sorting is easy
    output.sort(key=lambda i: i['dateStart'])

    # Add explicit index field
    for i, item in enumerate(output):
        item['speechIndex'] = i + 1

    # Store global dateStart/dateEnd
    sessionStart = output[0]['dateStart']
    sessionEnd = output[-1]['dateEnd']
    return { 'meta': { 'session': str(meeting_reference),
                       "processing": {
                           "parse_media": datetime.now().isoformat('T', 'seconds'),
                       },
                       'dateStart': sessionStart,
                       'dateEnd': sessionEnd },
             'data': output }

def parse_rss(filename: str, fixups: dict) -> list[dict]:
    """Parse a RSS file.
    """
    d = feedparser.parse(filename)

    return parse_media_data({ 'root': d,
                              'entries': d.entries }, fixups)

def parse_file(filename: str, fixups: dict) -> list[dict]:
    """Allow to parse either .xml files for raw .json files
    """
    filename = Path(filename)
    if filename.suffix == '.xml':
        return parse_rss(filename, fixups)
    elif filename.suffix == '.json':
        with open(filename) as f:
            raw_data = json.load(f)
        return parse_media_data(raw_data, fixups)
    else:
        logger.error(f"Unable to determine file type for {filename}")
        return []

def parse_media_directory(directory: Path):
    """Update parsed versions of media files.
    """
    for source in sorted(directory.glob('raw-*.json')):
        output_file = source.parent / source.name[4:]
        # If the output file does not exist, or is older than source file:
        if not output_file.exists() or output_file.stat().st_mtime < source.stat().st_mtime:
            source_data = json.loads(source.read_text())
            data = parse_media_data(source_data)
            logger.info(f"Converting {source.name}")
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parse Bundestag Media XML files or raw JSON files.")
    parser.add_argument("sources", type=str, nargs='*',
                        help="Source XML file")
    parser.add_argument("--fixups", type=argparse.FileType('r'),
                        help="Use fixups file (YAML)")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")

    args = parser.parse_args()
    if args.sources is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel)

    fixups = {}
    if args.fixups:
        fixups = yaml.safe_load(args.fixups)

    for source in args.sources:
        source = Path(source)
        if source.is_dir():
            parse_media_directory(source)
        else:
            data = parse_file(source, fixups)
            json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
