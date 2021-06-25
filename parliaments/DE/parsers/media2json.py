#! /usr/bin/env python3

# Convert RSS media index file from http://webtv.bundestag.de into JSON with
# fields defined for OpenParliamentTV

import logging
logger = logging.getLogger(__name__)

from datetime import datetime, timedelta
import feedparser
import json
import os
import re
import sys
from urllib.parse import urlparse, parse_qs

# Constants used for basic integrity checking: If these values are not
# present in the source data, then something must have changed and the
# parser should be checked anyway.
FEED_SUBTITLE = 'Deutscher Bundestag'
FEED_LICENSE = 'CC-BY-SA'
FEED_AUTHOR_EMAIL = 'mail@bundestag.de'
title_data_re = re.compile('Redebeitrag\s+von\s+(?P<fullname>.+?)\s+\((?P<faction>.+?)\)\s+am (?P<title_date>[\d.]+)\s+um\s+(?P<title_time>[\d:]+)\s+Uhr\s+\((?P<session_info>.+)\)')

def extract_title_data(title: str) -> dict:
    """Extract structured data from title string.

    Return a dict with fields if data could be extracted, else None.
    """
    # "Redebeitrag von Stephan Stracke (CDU/CSU) am 29.01.2010 um 14:05 Uhr (20. Sitzung, TOP ZP 2)"
    match = title_data_re.match(title)
    if match:
        return match.groupdict()
    else:
        return None

def fix_fullname(label: str) -> str:
    return label.replace('Dr. ', '').replace('h. c. ', '').replace('Prof. ', '')

def fix_title(title: str) -> str:
    """Fix the titles to match with proceedings conventions
    """
    title = title.replace("TOP Sitzungsende", "Sitzungsende").replace("TOP Sitzungseröffnung", "Sitzungseröffnung")
    zusatz = re.findall('TOP(?:\s+\d+)?,?\s+ZP\s+(\d+)', title)
    if zusatz:
        return f"Zusatzpunkt {zusatz[0]}"
    title = re.sub('^TOP\s+(.+)', 'Tagesordnungspunkt \\1', title)
    return title

def parse_rss(filename: str) -> dict:
    """Parse a RSS file.
    """
    output = []
    d = feedparser.parse(filename)

    # Do some validity checks
    if d['feed']['subtitle'] != FEED_SUBTITLE:
        logger.error(f"Feed subtitle is not {FEED_SUBTITLE}")
        return output
    if d['feed']['author_detail']['email'] != FEED_AUTHOR_EMAIL:
        logger.error(f"Feed author is not {FEED_AUTHOR_EMAIL}")
        return output

    # Convert links list to dict indexed by 'rel'
    session_links = dict( (l['rel'], l) for l in d['feed']['links'] )
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

    for e in d['entries']:
        links = dict( (l['rel'], l) for l in e ['links'] )

        if not 'enclosure' in links:
            # No media associated to the item.
            # FIXME: should we report the issue?
            logger.debug(f"No media associated to {filename}: {e['title']}")
            continue

        # Use duration to compute end time
        t = datetime.strptime(e['itunes_duration'],"%H:%M:%S")
        delta = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)

        startdate = datetime(*e['published_parsed'][:6])
        enddate = startdate + delta

        item = {
            "electoralPeriod": {
                "number": period_number,
            },
            "session": {
                "number": meeting_number,
                'dateStart': startdate.isoformat('T', 'seconds'),
                'dateEnd': enddate.isoformat('T', 'seconds'),
            },
            "agendaItem": {
                'title': e.get('description'),
                'officialTitle': fix_title(e['title']),
            },
            "media": {
                'videoFileURI': links['enclosure']['href'],
                'sourcePage': e['link'],
                'duration': delta.total_seconds(),
                'originID': os.path.basename(e['link']),
                'creator': e['author'],

                # Note: commented fields are defined in
                # https://github.com/OpenParliamentTV/OpenParliamentTV-Platform/issues/2
                # but not available here

                #'audioFileURI': '' ,
                #"thumbnailURI": "https://example.com/thumb.png",
                #"thumbnailCreator": "Deutscher Bundestag",
                #"thumbnailLicense": "CC-BY-SA",
                "license": FEED_LICENSE,
                # "originMediaID": "7502148",
                # "sourcePage": "https://dbtg.tv/fvid/7502148"
                'sourceFilename': filename,
            },
        }
        metadata = extract_title_data(e['title'])
        if metadata is not None:
            item['people'] = [
                {
                    'label': fix_fullname(metadata.get('fullname', '')),
                    'faction': metadata.get('faction', ''),
                }
            ]
            if metadata.get('session_info') is not None:
                # According to https://github.com/OpenParliamentTV/OpenParliamentTV-Parsers/issues/1
                # we should strip the Sitzung prefix from the session_info
                item['agendaItem']['officialTitle'] = fix_title(re.sub('^\d+\.\sSitzung,\s', '', metadata.get('session_info')))
            # FIXME: we have other fields: title_date, title_time that we could use for validation

        # Fix AgendaItemTitle if necessary
        if not item['agendaItem']['title']:
            title = fix_title(item['agendaItem']['officialTitle'])
            item['agendaItem']['title'] = title

        output.append(item)

    return output

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        logger.warning(f"Syntax: {sys.argv[0]} file.xml ...")
        sys.exit(1)

    data = [ item for source in sys.argv[1:] for item in parse_rss(source) ]
    # Sort data according to dateStart
    data.sort(key=lambda m: m['session']['dateStart'])
    json.dump(data, sys.stdout, indent=2, ensure_ascii=False)