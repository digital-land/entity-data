include makerules/makerules.mk
include makerules/pipeline.mk
include makerules/datapackage.mk

DB=dataset/digital-land.sqlite3

first-pass::
	mkdir -p dataset/
	bin/download-collection.sh
	bin/download-pipeline.sh
	bin/concat.sh
	bin/download-issues.sh
	#bin/download-resources.sh
	python3 bin/concat-issues.py

second-pass::	$(DB)

$(DB):	bin/load.py
	@rm -f $@
	python3 bin/load.py $@

datasette:	$(DB)
	datasette serve $(DB)

clean::
	rm -rf ./var

clobber::
	rm -rf var/collection
	rm -rf var/pipeline
	rm -rf dataset/
	rm -rf $(DB)
