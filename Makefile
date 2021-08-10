include makerules/makerules.mk

first-pass::
	bin/download.sh
	bin/concat.sh


clean::
	rm -rf ./var

