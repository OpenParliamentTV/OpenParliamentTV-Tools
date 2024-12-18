#! /usr/bin/env python3

# Extract transcript from data files from http://webtv.bundestag.de
# into JSON

# It output an array of items, each items represents a speech (rede) with additionnal metadata


import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
from itertools import takewhile
import json
from lxml import etree
from pathlib import Path
import re
from spacy.lang.de import German
import sys

# Allow relative imports if invoked as a script
# From https://stackoverflow.com/a/65780624/2870028
if __package__ is None:
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

from .common import fix_faction, fix_fullname, parse_fullname

PROCEEDINGS_LICENSE = "Public Domain"
PROCEEDINGS_LANGUAGE = "DE-de"

SPEECH_CLASSES = set(('J', 'J_1', 'O', 'T_NaS', 'T_fett', 'Z'))

LEADING_SPEECH = '-intro'
TRAILING_SPEECH = '-outro'
CLOSING_SPEECH = '-closing'
VIRTUAL_SPEECH = '-post'

ddmmyyyy_re = re.compile(r'(?P<dd>\d\d)\.(?P<mm>\d\d)\.(?P<yyyy>\d\d\d\d)')
time_re = re.compile(r'(?P<h>\d?\d)[\.:](?P<m>\d\d)')

# Global language model - to save load time
nlp = German()
# sentencizer is a rule-based sentencizer. It has less dependencies
# than the model-based one.
nlp.add_pipe("sentencizer")

def fix_time(t: str) -> str:
    """Fix a sitzung-start/ende-uhrzeit

    Return a ISO-formatted timecode (with :00 seconds if needed)

    It sometimes has wrong separator, wrong 0-padded info or spurious
    data (e.g. "13.00 Uhr" in 19117) in some files
    """
    match = time_re.match(t)
    if match:
        h = int(match['h'])
        m = int(match['m'])
        return f"{h:02}:{m:02}:00"
    else:
        logger.warning(f"Error in timecode: {t}")
        return f"ERROR:{t}"

def clean_text(t: str) -> str:
    """Clean text before splitting into sentences.
    """
    t = re.sub(r'\n\s+', ' ', t)
    return t

def parse_speakers(speakers):
    """Convert a list a list of <redner> to a dict of Person data indexed by fullname
    """
    result = {}
    identifiers = set()
    for s in speakers:
        ident = s.attrib['id']
        if ident in identifiers:
            # Already parsed/present in the list
            continue
        identifiers.add(ident)
        firstname = s.findtext('.//vorname') or ""
        lastname = s.findtext('.//nachname') or ""
        nameaddition = s.findtext('.//namenszusatz') or ""
        fullname = f"{firstname} {nameaddition} {lastname}"
        fullname, status = parse_fullname(fullname)
        faction = fix_faction(s.findtext('.//fraktion') or "")
        # Persons can be without any party (independent) but join a faction. So we cannot assume any correspondence between both.
        #party = faction.split('/')[0]

        result[fullname] = {
            'fullname': fullname,
            'firstname': firstname,
            'lastname': lastname,
            'identifier': ident
        }
        # Add faction attribute if it is not empty
        if faction:
            result[fullname]['faction'] = faction

    return result

def split_sentences(paragraph: str) -> list:
    doc = nlp(paragraph)
    return [ { 'text': str(sent).strip() } for sent in doc.sents ]

def parse_speech(elements: list, last_speaker: dict, speech_id: str):
    # speaker/speakerstatus are initialized from the calling method
    # speakerstatus: president / vice-president / main-speaker / speaker
    speaker = last_speaker['speaker']
    speakerstatus = last_speaker['speakerstatus']

    # Memorize main_speaker for the session, so that other speakers that may intervene in the same speech are classified as 'speaker'
    main_speaker = None
    for c in elements:
        if c.tag == 'name':
            # Pr/VP name, strip trailing :
            speaker, status = parse_fullname(c.text)
            speakerstatus = status or "speaker"
            continue
        if c.tag == 'kommentar':
            yield {
                    'speech_id': speech_id,
                    'type': 'comment',
                    'speaker': None,
                    'speakerstatus': None,
                    'text': clean_text(c.text),
                    'sentences': [
                        { 'text': clean_text(c.text) }
                    ]
                }
            continue
        if c.tag == 'p':
            klasse = c.attrib.get('klasse')
            if klasse == 'redner':
                # Speaker identification
                firstname = c.findtext('.//vorname') or ""
                lastname = c.findtext('.//nachname') or ""
                nameaddition = c.findtext('.//namenszusatz') or ""
                speaker = f"{firstname} {nameaddition} {lastname}"
                speaker, status = parse_fullname(speaker)
                if status is not None:
                    speakerstatus = status
                elif main_speaker is None:
                    main_speaker = speaker
                    speakerstatus = 'main-speaker'
                else:
                    if main_speaker == speaker:
                        speakerstatus = 'main-speaker'
                    else:
                        # Plain speaker - there is already a main_speaker for the speech
                        speakerstatus = 'speaker'
                continue
            elif klasse == 'N':
                # Speaker name - Präsident or Vizepräsident
                speech_text = c.text
                if speech_text:
                    # Remove any leading or trailing spaces
                    speech_text = speech_text.strip()

                    # Remove any additional tags inside the 'p' tag
                    for element in c.iterdescendants():
                        if element.tag != 'a':
                            speech_text = speech_text.replace(element.text, '')

                    # Parse the speaker and status from the cleaned text
                    speaker, status = parse_fullname(speech_text)
                    continue
            elif klasse in SPEECH_CLASSES and c.text:
                # Actual text. Output it with speaker information.
                yield {
                    'speech_id': speech_id,
                    'type': 'speech',
                    'speaker': speaker,
                    'speakerstatus': speakerstatus,
                    'text': clean_text(c.text),
                    'sentences': split_sentences(clean_text(c.text))
                }
            # FIXME: all other <p> klasses are ignored for now

def parse_ordnungpunkt(op, last_speaker: dict, last_redeid: str, session_id: str):
    """Parse an <tagesordnungspunkt> to output a sequence of tagged speech items.

    It is a generator that generates 1 array of speech items by rede.

    Each tagesordnungspunkt has a number of speeches (rede), each having a main-speaker (redner)

    Speaker names can be specified in multiple ways:
    - either <p klasse="redner"> which contains the full redner identification
    - or <p klasse="N"> which contains a name (mostly for Präsident)
    - or a <name> tag (mostly for Präsident)
    - or sometimes in freeform in <kommentar> like "(Steffi Lemke [BÜNDNIS 90/DIE GRÜNEN]: Da freut sich die FDP auch drüber!)" (ignored for now)

    so we have to go through items in order and maintain a "speaker" state variable.

    On top of that, op may contain <rede> or <p> children (and <rede> contains <p>)
    """

    # import IPython; IPython.embed()

    # An ordnungpunkt normally consists of multiple <rede>.

    # But at the beginning there may be an introduction by the
    # president, in the form of multiple <p>. If this is the case,
    # produce a virtual <rede> called Introduction.

    # Consider only p or rede elements
    elements = [ node for node in op if node.tag in ('p', 'name', 'rede') ]

    # Get rede id from first rede node
    first_rede = next( (e for e in elements if e.tag == 'rede'), None)
    if first_rede is not None:
        rede_id = first_rede.attrib.get('id', '')
    else:
        # No <rede> node at all. It often happens in sitzungsbeginn/sitzungsende
        if op.tag == 'sitzungsbeginn':
            rede_id = 'begin'
        elif op.tag == 'sitzungsende':
            rede_id = 'ende'
        else:
            rede_id = f"{last_redeid}{VIRTUAL_SPEECH}"

    # Produce a virtual introduction
    introduction = list(takewhile(lambda n: n.tag in ('p', 'name'), elements))
    if introduction:
        turns = list(parse_speech(introduction, last_speaker, f"{rede_id}{LEADING_SPEECH}"))
        if turns:
            last_speaker = last_speaker_info(turns)
            yield turns

    for el in elements:
        if el.tag != 'rede':
            # We just processed leading <p>. There may remain some
            # trailing <p>, which we ignore for now
            continue
        # Get the rede id from original proceedings
        rede_id = el.attrib.get('id', 'unknown_redeid')
        turns = list(parse_speech(el, last_speaker, rede_id))
        if turns:
            last_speaker = last_speaker_info(turns)
            yield turns

    # If first_rede is None, then the non-rede items will have been processed in the introduction handling.
    # We do not want to process then again.
    if first_rede is not None:
        # Trailing <p> elements after last <rede>
        closing = list(reversed(list(takewhile(lambda n: n.tag in ('p', 'name'), reversed(elements)))))
        if closing:
            turns = list(parse_speech(closing, last_speaker, f"{rede_id}{TRAILING_SPEECH}"))
            if turns:
                last_speaker = last_speaker_info(turns)
                yield turns

def parse_documents(op):
    for doc in op.findall('p[@klasse="T_Drs"]'):
        # There may be multiple Drucksache in a single .T_Drs:
        # "Drucksachen 19/27871, 19/27822, 19/27315, 19/29694"
        for session, ref in re.findall(r'(\d\d)/(\d+)', doc.text):
            padded = ref.rjust(5, '0')
            yield {
                "type": "officialDocument",
                "label": f"Drucksache {session}/{ref}",
                "sourceURI": f"https://dserver.bundestag.de/btd/{session}/{padded[:3]}/{session}{padded}.pdf"
            }


def ddmmyyyy_to_iso(date):
    """Convert from dd.mm.yyyy to iso format YYYY-MM-DD

    Returns the unmodified input date if it does not match
    """
    if date:
        match = ddmmyyyy_re.match(date)
        if match:
            d = match.groupdict()
            date = f"""{d['yyyy']}-{d['mm']}-{d['dd']}"""
    return date

def time_to_int(t):
    """Convert a time HH:MM into a number of minutes.
    """
    try:
        # Normally time is HH:MM but in some files (like 19081) the
        # separator is .
        h, m = re.split(r'[:\.]', t)
    except IndexError:
        # Single value
        return 0
    return int(m) + 60 * int(h)

def last_speaker_info(turns):
    # Find the last turn item for which speaker is not null
    # (it may be a comment)
    sp = [ t
           for t in turns
           if t['speaker'] is not None ]
    if sp:
        return {
            'speaker': sp[-1]['speaker'],
            'speakerstatus': sp[-1]['speakerstatus']
        }
    else:
        return {
            'speaker': None,
            'speakerstatus': None
        }

def fix_last_speech(speeches):
    """Try to fix last speech issue:

    If the last speech in an ordnungspunkt has trailing items by
    president, then generate a new speech with them because usually,
    the media splitting is done at this moment.
    """
    if len(speeches) < 2:
        return speeches
    last_speech = speeches[-1]
    trailing_president_items = list(reversed(list(turn
                                                  for turn in reversed(last_speech)
                                                  if (turn.get('speakerstatus') or "").endswith('president')
                                                  )))
    trailing_count = len(trailing_president_items)
    # Do not consider cases where *president is the only speaker
    if trailing_count != len(last_speech):
        # Split this speech into 2 different ones
        logger.debug("Splitting president speech from last TOP speech")
        speeches = speeches[:-1]
        speeches.append(last_speech[:-(trailing_count+1)])
        # We generate a pseudo-speech, so we also have to generate a pseudo-speech-id
        for i in trailing_president_items:
            i['speech_id'] = f"{i['speech_id']}{CLOSING_SPEECH}"
        speeches.append(trailing_president_items)
    return speeches

def parse_transcript(filename: str, sourceUri: str = None, args=None):
    # We are mapping 1 self-contained object/structure to each tagesordnungspunkt
    # This method is a generator that yields tagesordnungspunkt structures
    # Make sure to convert to str if a Path is given
    filename = str(filename)
    if sourceUri is None:
        sourceUri = filename
    tree = etree.parse(filename)
    root = tree.getroot()

    # Try to get source URL information
    source_urls = [ n.attrib.get('url')
                    for n in root.xpath("preceding-sibling::node()")
                    if getattr(n, 'target') == 'source' ]
    # Source URL has been stored into the XML file
    if source_urls:
        sourceUri = source_urls[0]

    intro = root.find('vorspann')
    metadata = intro.find('kopfdaten')

    date = ddmmyyyy_to_iso(root.attrib.get('sitzung-datum', ''))
    nextDate = ddmmyyyy_to_iso(root.attrib.get('sitzung-naechste-datum', ''))
    timeStart = fix_time(root.attrib.get('sitzung-start-uhrzeit', ''))
    timeEnd = fix_time(root.attrib.get('sitzung-ende-uhrzeit', ''))

    # Note: these are local time, naive timestamps.
    # We will get the UTC offset info from media and update the dates
    # in the merging phase.
    dateStart = f"{date}T{timeStart}"
    dateEnd = f"{date}T{timeEnd}"
    # String comparison works correctly since we 0-padded it.
    if timeEnd < timeStart:
        # end time < start time: this is a session that went after
        # midnight, and ends on the next day - fix the dateEnd
        dateEnd = f"{nextDate}T{timeEnd}"

    period = metadata.findtext('.//wahlperiode')
    session = metadata.findtext('.//sitzungsnr')

    # Generate a session id that will be prefixed to rede ids to generate speech_id
    session_id = f"{period}{session.zfill(3)}"

    # metadata common to all tagesordnungspunkt
    session_metadata = {
        "parliament": "DE",
        'electoralPeriod': {
            'number': period,
        },
        'session': {
            'number': session,
            'dateStart': dateStart,
            'dateEnd': dateEnd,
        },
    }

    # Store speaker dict, but only of <redner> nodes under <sitzungsverlauf>
    # Otherwise we also get the list of speakers in the attachments which often contains mistakes
    speaker_info = parse_speakers(root.find('sitzungsverlauf').findall('.//redner'))

    last_speaker = {
        'speaker': "Unknown",
        'speakerstatus': "Unknown"
    }

    # Start index at 1001 so that we can distinguish btw media and proceedings indexes
    # (starting at 1001 and not 1000 because media starts at 1)
    speechIndex = 1001
    last_redeid = 'sessionstart'
    # Pass last speaker info from one speech to the next one
    for op in [ *root.findall('.//sitzungsbeginn'),
                *root.findall('.//tagesordnungspunkt'),
                *root.findall('.//sitzungsende') ]:
        speeches = list(parse_ordnungpunkt(op, last_speaker, last_redeid, session_id))
        if op.tag == 'sitzungsbeginn':
            title = 'Sitzungseröffnung'
        elif op.tag == 'sitzungsende':
            title = 'Sitzungsende'
        else:
            title = op.attrib['top-id']

        if speeches:
            # Use turn info from last speech to get last speaker
            last_speaker = last_speaker_info(speeches[-1])

        speeches = fix_last_speech(speeches)

        documents = list(parse_documents(op))

        # Yield 1 structure per speech
        for speech in speeches:
            if not speech:
                # No speech item. Do not generate  an item
                continue
            # Extract list of speakers for this speech
            speakerstatus_dict = dict( (turn['speaker'], turn['speakerstatus'])
                                       for turn in speech
                                       # Do not consider null speakers (for comments)
                                       if turn['speaker'] )
            def speaker_item(fullname, status):
                info = speaker_info.get(fullname)
                if info:
                    return {
                        # FIXME: this could be memberOfGovernment / Other
                        # But this information is present in media, not in proceedings
                        "type": "memberOfParliament",
                        "label": fix_fullname(fullname),
                        "firstname": info['firstname'],
                        "lastname": info['lastname'],
                        "context": status,
                        "faction": info.get('faction', ''),
                    }
                else:
                    return {
                        # FIXME: this could be memberOfGovernment / Other
                        "type": "memberOfParliament",
                        "label": fix_fullname(fullname),
                        "context": status
                    }
            # Sort speakers so that main speaker is always first
            speakers = list(sorted(  ( speaker_item(fullname, status)
                                       for fullname, status in speakerstatus_dict.items() ),
                                     key=lambda si: 0 if si['context'] == 'main-speaker' else 1))

            speech_id = speech[0]['speech_id']
            last_redeid = speech_id

            yield {
                **session_metadata,
                "speechIndex": speechIndex,
                "originTextID": speech_id,
                'agendaItem': {
                    "officialTitle": title,
                    # The human-readable title is not present in proceedings, it will be in media
                    # "title": title,
                },
                "debug": {
                },
                'people': speakers,
                'textContents': [
                    {
                        "type": "proceedings",
                        "sourceURI": sourceUri,
                        "creator": metadata.findtext('.//herausgeber'),
                        "license": PROCEEDINGS_LICENSE,
                        "language": PROCEEDINGS_LANGUAGE,
                        "originTextID": speech_id,
                        "textBody": speech,
                    }
                ],
                'documents': documents,
            }
            speechIndex += 1

def get_parsed_proceedings_filename(source: str, output: str) -> Path:
    output_dir = Path(output)
    if not output_dir.is_dir():
        output_dir.mkdir(parents=True)
    basename = Path(source).stem
    return output_dir / f"{basename}.json"

def parse_proceedings(source: str, output: str, uri: str, args):
    """Parse the proceedings file source and store the output in the output directory.
    """
    speeches = list(parse_transcript(source, uri, args))
    # Get session id from first item
    speech = speeches[0]
    period = speech['electoralPeriod']['number']
    meeting = speech['session']['number']
    session_id = f"{period}{str(meeting).zfill(3)}"

    data = { "meta": { "session": session_id,
                       "processing": {
                           "parse_proceedings": datetime.now().isoformat('T', 'seconds'),
                       },
                       'dateStart': speech['session']['dateStart'],
                       'dateEnd': speech['session']['dateEnd'],
                      },
             "data": speeches }
    if output == "-":
        # Dump to stdout
        json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
    elif output:
        output_file = get_parsed_proceedings_filename(source, output)
        logger.debug(f"Saving to {output_file}")
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    return data

def parse_proceedings_directory(directory: Path, args):
    """Update parsed versions of proceedings files.
    """
    for source in sorted(directory.glob('*.xml')):
        output_file = get_parsed_proceedings_filename(source, directory)
        # If the output file does not exist, or is older than source file:
        if not output_file.exists() or output_file.stat().st_mtime < source.stat().st_mtime:
            # Since we do not know the source URI, we specify the local filename
            parse_proceedings(source, directory, str(source), args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parse Bundestag Proceedings XML files.")
    parser.add_argument("source", type=str, nargs='?',
                        help="Source XML file")
    parser.add_argument("--uri", type=str,
                        help="Origin URI")
    parser.add_argument("--output", type=str, default="-",
                        help="Output directory")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        default=False,
                        help="Display debug messages")
    args = parser.parse_args()
    if args.source is None:
        parser.print_help()
        sys.exit(1)
    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel)

    source = Path(args.source)
    if source.is_dir():
        parse_proceedings_directory(source, args)
    else:
        parse_proceedings(args.source, args.output, args.uri, args)
