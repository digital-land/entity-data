include makerules/makerules.mk
include makerules/pipeline.mk

DB=digital-land.db

first-pass::
	mkdir -p dataset/
	bin/download.sh
	bin/concat.sh
	bin/download-issues.sh
	bin/download-resources.sh
	python3 bin/concat-issues.py

second-pass::	digital-land.db

digital-land.db:	bin/load.py
	@rm -f $@
	python3 bin/load.py $@

datasette:	digital-land.db
	datasette serve digital-land.db

clean::
	rm -rf ./var

clobber::
	rm -rf var/collection/
	rm -rf dataset/
	rm -rf collection.db
