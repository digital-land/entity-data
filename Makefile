include makerules/makerules.mk

first-pass::
	bin/download.sh
	bin/concat.sh
	bin/download-issues.sh
	bin/download-resources.sh

clean::
	rm -rf ./var

