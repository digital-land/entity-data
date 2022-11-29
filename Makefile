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
	#bin/download-issues.sh
	#bin/download-resources.sh
	#python3 bin/concat-issues.py

second-pass::	$(DB)

$(DB):	bin/load.py
	@rm -f $@
	python3 bin/load.py $@

clean::
	rm -rf ./var

clobber::
	rm -rf var/collection
	rm -rf var/pipeline
	rm -rf dataset/
	rm -rf $(DB)

aws-build::
	aws batch submit-job --job-name digital-land-db-$(shell date '+%Y-%m-%d-%H-%M-%S') --job-queue dl-batch-queue --job-definition dl-batch-def --container-overrides '{"environment": [{"name":"BATCH_FILE_URL","value":"https://raw.githubusercontent.com/digital-land/docker-builds/main/builder_run.sh"}, {"name" : "REPOSITORY","value" : "digital-land-builder"}]}'

push::
	aws s3 cp $(DB) s3://digital-land-collection/digital-land.sqlite3

specification::
	# additional
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/issue-type.csv' > specification/issue-type.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/severity.csv' > specification/severity.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/project.csv' > specification/project.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/specification.csv' > specification/specification.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/specification-status.csv' > specification/specification-status.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/organisation-dataset.csv' > specification/organisation-dataset.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/project-status.csv' > specification/project-status.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/provision-reason.csv' > specification/provision-reason.csv
