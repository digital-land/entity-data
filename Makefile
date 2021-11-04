include makerules/makerules.mk
include makerules/pipeline.mk
include makerules/datapackage.mk
include makerules/development.mk

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

aws-build::
	aws batch submit-job --job-name digital-land-db-$(shell date '+%Y-%m-%d-%H-%M-%S') --job-queue dl-batch-queue --job-definition dl-batch-def --container-overrides '{"environment": [{"name":"BATCH_FILE_S3_URL","value":"s3://dl-batch-scripts/digital-land-builder.sh"}]}'
