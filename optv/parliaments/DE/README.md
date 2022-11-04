# Scraper/parser architecture

The `scraper` package holds modules/scripts aimed at scraping data
from websites.

The `parser` package holds modules/scripts that extract information
from the scraped files, and converts them into a unified JSON format.

The `merger` package holds modules/scripts for merging information
from transformed files.

# General instructions

The main entry point is the `workflow.py` module, which will fetch
media and proceedings data, parse them, merge files as needed and
optionally apply time alignment (with `--align-sentences`) and NER
(with `--extract-entities`). You have to specify at least the output
directory as well as the period number:

`./workflow.py --period=20 data`

Additionally, a `Makefile` automates the download and merging phases
and can allow more fine-grained control. You can run

`make download`

to first download media and proceedings files, then

`make`

to merge the downloaded files.

In addition, the various components for fetching, parsing and merging
data can be invoked independently. For instance, you can use the
`merger/merge_session.py` to force the merging of specific files, or
of the whole media/proceedings directories.

# Environment setup

Some modules have external dependencies (for RSS parsing, sentence
splitting...). The command `python3 -m pip install -r
requirements.txt` will install the necessary requirements.

# Scraping data

For operational reference, see the `download` target of the Makefile.
Running `make download` will execute the 2 steps involved.

There are for the moment 2 data sources: proceedings and media.

Proceedings are fetched by the `scraper/fetch_proceedings.py` script,
into the `data/original/proceedings` directory.

Media data can be fetched by the `scraper/fetch_media.py` script, by
providing period and meeting numbers - it will handle feed
pagination.

This script is used by the `update_media` script, which can use either
the `proceedings` directory content (`--from-proceedings` option) to
determine the appropriate period and meeting numbers, or a given
period number (`--from-period` option). It will download the
corresponding media data, directly in unified json format.

The Bundestag media server regularly has trouble downloading specified
period/meeting data (the server often returns a 503 code, like some
kind of timeout in building data). Providing the `--retry-count`
option with an number of maximum retries will make `update_media` try
again a number of times in case of errors, with a random-length
timeout (from 0 to 10s) between each try. Additionally, the
`update_media` script will by default only try to download files that
are not already existing in the output directory.  It can thus be run
multiple times if necessary.
