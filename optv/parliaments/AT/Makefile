MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-AT

# `download` with no SITTING walks the whole period to discover sittings;
# pass SITTING="144 200 257" to target specific ones:
#   make download SITTING="144 200 257"
SITTING ?=

all:
	$(BASEDIR)/workflow.py --period=27 "$(DATADIR)" \
	    --merge-speeches --link-entities --align-sentences --extract-entities

download:
	$(BASEDIR)/workflow.py --period=27 "$(DATADIR)" --download-original \
	    $(foreach s,$(SITTING),--sitting $(s)) --merge-speeches

merge:
	$(BASEDIR)/workflow.py --period=27 "$(DATADIR)" --merge-speeches

entities:
	cd $(BASEDIR)../../.. && .venv/bin/python -m optv.parliaments.AT.scraper.build_entities "$(DATADIR)"

update: all

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
