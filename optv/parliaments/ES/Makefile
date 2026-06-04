MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-ES
MEDIADIR := $(DATADIR)/original/media
PROCEEDINGSDIR := $(DATADIR)/original/proceedings
MERGEDDIR := $(DATADIR)/cache/merged

PERIOD := 15

# Proceedings are per-session HTML Diarios; media is the per-session raw
# interventions JSON. Both parsed JSONs are produced from those sources.
TRANSCRIPT_SOURCE := $(wildcard $(PROCEEDINGSDIR)/*-proceedings.html)
MEDIA_SOURCE := $(wildcard $(MEDIADIR)/raw-*-media.json)

TRANSCRIPT_DEST := $(patsubst %-proceedings.html,%-proceedings.json,$(TRANSCRIPT_SOURCE))
MEDIA_DEST := $(subst raw-,,$(MEDIA_SOURCE))

# Use proceedings as the merge reference (one merged file per session)
MERGE_DEST := $(subst /original/proceedings/,/cache/merged/,$(patsubst %-proceedings.json,%-merged.json,$(TRANSCRIPT_DEST)))

all: $(MEDIA_DEST) $(TRANSCRIPT_DEST) $(MERGE_DEST)

download:
	$(BASEDIR)/workflow.py --period=$(PERIOD) "$(DATADIR)" --download-original --no-merge-speeches

update: download all

debug:
	-echo "MEDIA_DEST"
	-echo $(MEDIA_DEST)
	-echo "MERGE DEST"
	-echo $(MERGE_DEST)

%-merged.json: ../../original/proceedings/%-proceedings.json ../../original/media/%-media.json
	$(BASEDIR)/merger/merge_session.py --output=$(DATADIR)/cache/merged $^

%-media.json: raw-%-media.json
	$(BASEDIR)/parsers/media2json.py $< > $@

%-proceedings.json: %-proceedings.html
	$(BASEDIR)/parsers/proceedings2json.py $< > $@

clean:
	$(RM) $(MEDIADIR)/[0-9]*.json
	$(RM) $(PROCEEDINGSDIR)/*.json
	$(RM) $(MERGEDDIR)/*.json

test:
	cd $(BASEDIR)../../.. && .venv/bin/pytest -q tests
