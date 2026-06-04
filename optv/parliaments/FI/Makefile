MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-FI

# `--period` is the vaalikausi (term) start year. NER is intentionally absent
# (entity-fishing ships no Finnish KB); the pipeline runs download → parse →
# merge → NEL → align.

all:
	$(BASEDIR)/workflow.py --period=2023 "$(DATADIR)" \
	    --download-original --merge-speeches --link-entities --align-sentences

download:
	$(BASEDIR)/workflow.py --period=2023 "$(DATADIR)" --download-original

merge:
	$(BASEDIR)/workflow.py --period=2023 "$(DATADIR)" --merge-speeches --link-entities

update: all

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
