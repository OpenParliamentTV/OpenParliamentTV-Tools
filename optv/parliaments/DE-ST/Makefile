MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-ST
PROCEEDINGSDIR := $(DATADIR)/original/proceedings
MEDIADIR := $(DATADIR)/original/media
MERGEDDIR := $(DATADIR)/cache/merged
METADIR := $(DATADIR)/metadata

# Proceedings are downloaded per-Sitzungsperiode (sp-NNN.html), parsed into
# per-Sitzung JSON (08NNN-proceedings.json). Media is one subdir per Sitzung
# holding per-player-id AJAX HTML, parsed into 08NNN-media.json.
PAGE_SOURCE := $(wildcard $(PROCEEDINGSDIR)/sp-*.html)
PROCEEDINGS_DEST := $(wildcard $(PROCEEDINGSDIR)/08*-proceedings.json)
MEDIA_DEST := $(patsubst %,$(MEDIADIR)/%.json,$(notdir $(basename $(PROCEEDINGS_DEST:-proceedings.json=-media))))
MERGE_DEST := $(patsubst %-proceedings.json,$(MERGEDDIR)/%-merged.json,$(notdir $(PROCEEDINGS_DEST)))

all: download $(MERGE_DEST)

download:
	$(BASEDIR)/workflow.py --period=8 --download-original --no-merge-speeches "$(DATADIR)"

update:
	$(BASEDIR)/update "$(DATADIR)"

debug:
	-echo "PAGE_SOURCE: $(PAGE_SOURCE)"
	-echo "PROCEEDINGS_DEST: $(PROCEEDINGS_DEST)"
	-echo "MEDIA_DEST: $(MEDIA_DEST)"
	-echo "MERGE_DEST: $(MERGE_DEST)"

clean:
	$(RM) $(PROCEEDINGSDIR)/08*-proceedings.json
	$(RM) $(MEDIADIR)/08*-media.json
	$(RM) $(MERGEDDIR)/08*-merged.json
