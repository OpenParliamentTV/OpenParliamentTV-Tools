#! /usr/bin/env python3

import logging
logger = logging.getLogger(__name__)

import chevron
from http.server import HTTPServer, SimpleHTTPRequestHandler
import io
import json
import mimetypes
from pathlib import Path
import sys

from merge_session import unmatched_count

HOST_NAME = "0.0.0.0"
HOST_PORT = 3333

DATA_DIR = Path(__file__).resolve().parent.parent / 'data' / 'merged'
TEMPLATE_DIR = Path(__file__).resolve().parent / 'templates'

class SessionServer(SimpleHTTPRequestHandler):
    def _set_headers(self, mimetype="text/html; charset=utf-8"):
        self.send_response(200)
        self.send_header('Content-type', mimetype)
        self.end_headers()

    def index(self, fd):
        with open(TEMPLATE_DIR / 'index.mustache') as template:
            fd.write(chevron.render(template, {
                "merged_files": [
                    {
                        "name": f.name
                    }
                    for f in sorted(DATA_DIR.glob('*.json'), reverse=True)
                ]
            }))

    def dump_file(self, fd, fname):

        def template_data(source):
            for speech in source:
                # Only consider speech turns (ignoring comments)
                if 'textContents' not in speech:
                    # No proceedings data, only media.
                    speech_turns = []
                    message = "MEDIA ONLY"
                else:
                    speech_turns = [ turn for turn in speech['textContents'][0]['textBody'] if turn['type'] == 'speech' ]
                    president_turns = [ turn for turn in speech_turns if turn['speakerstatus'].endswith('president') ]
                    if len(president_turns) == len(speech_turns):
                        # Homogeneous president turns
                        message = "PRESIDENT ONLY"
                    else:
                        message = ""
                yield {
                    "index": speech['agendaItem']['speechIndex'],
                    "title": speech['agendaItem']['officialTitle'],
                    "speech_turns": speech_turns,
                    "message": message,
                    "videoURI": speech.get('media', {}).get('videoFileURI', "")
                }

        with open(DATA_DIR / fname, 'r') as f:
            data = json.load(f)

        speeches = list(template_data(data))
        with open(TEMPLATE_DIR / 'transcript.mustache') as template:
            fd.write(chevron.render(template, {
                "session": fname,
                "speeches": speeches,
                "speeches_json": json.dumps(data),
                "speech_count": len(speeches),
                "unmatched_count": len([s for s in speeches if not s['speech_turns']])
            }))
        return

    def stat_files(self, fd, fnames):
        """Display stats for given filenames
        """
        rows = []
        for merged in fnames:
            # Find session id
            session = str(merged.name)[:5]
            # Consider a standard directory layout
            proceeding = merged.parent.parent / 'proceedings' / f'{session}-data.json'
            media = merged.parent.parent / 'media' / f'{session}-media.json'
            counts = unmatched_count(proceeding, media, dict())
            counts.update(session=session)
            rows.append(counts)

        headings = list(rows[0].keys())
        rows = [ [ r.get(h) for h in headings ] for r in rows ]
        with open(TEMPLATE_DIR / 'stat_files.mustache') as template:
            fd.write(chevron.render(template, {
                "filenames": fnames,
                "count": len(fnames),
                "rows": rows,
                "rows_json": json.dumps(rows),
                "headings": headings,
                "headings_json": json.dumps(headings)
            }))

    def do_GET(self):
        self.out = io.TextIOWrapper(
            self.wfile,
            encoding='utf-8',
            line_buffering=False,
            write_through=True,
        )
        if self.path == '' or self.path == '/':
            self._set_headers()
            self.index(self.out)
            return
        elif self.path.startswith('/static/'):
            path = self.path
            if '?' in path:
                path = path.split('?')[0]
            resource = TEMPLATE_DIR / path[1:]
            if resource.exists():
                self._set_headers(mimetypes.guess_type(resource))
                self.out.write(resource.read_text())
            else:
                self.send_response(404)
                self.end_headers()
        elif self.path.startswith('/data/'):
            resource = DATA_DIR / '..' / self.path[6:]
            if resource.is_dir():
                self._set_headers('text/plain')
                self.out.write("\n".join(sorted(n.name for n in resource.glob('*'))))
            elif resource.exists():
                self._set_headers(mimetypes.guess_type(resource))
                self.out.write(resource.read_text())
            else:
                self.send_response(404)
                self.end_headers()
        elif self.path.startswith('/view/'):
            fname = self.path.split('/')[2]
            self._set_headers()
            self.dump_file(self.out, fname)
            return
        elif self.path.startswith('/stats/'):
            fname = self.path.split('/')[2]
            self._set_headers()
            fnames = list(sorted(DATA_DIR.glob(f'{fname}*.json'), reverse=True))
            self.stat_files(self.out, fnames)
            return
        else:
            SimpleHTTPRequestHandler.do_GET(self)

def main():
    httpserver = HTTPServer((HOST_NAME, HOST_PORT), SessionServer)

    try:
        httpserver.serve_forever()
    except KeyboardInterrupt:
        pass
    httpserver.server_close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) > 1:
        DATA_DIR = Path(sys.argv[1]).resolve()
    logger.info(f"Listening to {HOST_NAME}:{HOST_PORT}")
    main()