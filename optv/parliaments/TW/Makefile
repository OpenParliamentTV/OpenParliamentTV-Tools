MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-TW

# Note: download requires an explicit --session <code> (one or more).
# Example: ./workflow.py --period=11 $(DATADIR) --download-original \
#              --session 院會-11-5-11

all:
	$(BASEDIR)/workflow.py --period=11 "$(DATADIR)" \
	    --merge-speeches --align-sentences --link-entities --extract-entities

merge:
	$(BASEDIR)/workflow.py --period=11 "$(DATADIR)" --merge-speeches

update: all

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
