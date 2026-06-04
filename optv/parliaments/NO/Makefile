MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-NO

all:
	$(BASEDIR)/workflow.py --period=22 "$(DATADIR)" \
	    --download-original --merge-speeches \
	    --link-entities --align-sentences --extract-entities

download:
	$(BASEDIR)/workflow.py --period=22 "$(DATADIR)" --download-original

merge:
	$(BASEDIR)/workflow.py --period=22 "$(DATADIR)" --merge-speeches

entities:
	python3 -m optv.parliaments.NO.scraper.build_entity_dump "$(DATADIR)" --period 22

update: all

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
