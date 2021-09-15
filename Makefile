include makerules/makerules.mk
include makerules/pipeline.mk

first-pass::
	mkdir -p dataset/
	bin/download.sh
	bin/concat.sh
	bin/download-issues.sh
	bin/download-resources.sh
	python3 bin/concat-issues.py

second-pass::	collection.db

collection.db:	bin/load.py
	@rm -f $@
	python3 bin/load.py $@

datasette:	collection.db
	datasette serve collection.db # pipeline.db logs.db

clean::
	rm -rf ./var

clobber::
	rm -rf var/collection/
	rm -rf dataset/
	rm -rf collection.db
