MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-EU

all:
	$(BASEDIR)/workflow.py --period=10 "$(DATADIR)"

download:
	$(BASEDIR)/workflow.py --period=10 "$(DATADIR)" --download-original

merge:
	$(BASEDIR)/workflow.py --period=10 "$(DATADIR)" --merge-speeches

update: download merge

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
