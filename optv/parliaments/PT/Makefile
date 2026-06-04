MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-PT

all:
	$(BASEDIR)/workflow.py --period=17 "$(DATADIR)"

download:
	$(BASEDIR)/workflow.py --period=17 "$(DATADIR)" --download-original

merge:
	$(BASEDIR)/workflow.py --period=17 "$(DATADIR)" --merge-speeches

update: download merge

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
