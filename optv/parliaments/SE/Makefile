MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-SE

# Note: there is no `download` target because Riksdag downloads require an
# explicit --session <dok_id> (see workflow.py). Use workflow.py directly:
#   ./workflow.py --period=2025 $(DATADIR) --download-original --session HD0991

all:
	$(BASEDIR)/workflow.py --period=2025 "$(DATADIR)" \
	    --merge-speeches --align-sentences --extract-entities

merge:
	$(BASEDIR)/workflow.py --period=2025 "$(DATADIR)" --merge-speeches

update: all

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
