include makerules/makerules.mk

first-pass::
	mkdir -p dataset/
	bin/download.sh
	bin/concat.sh
	bin/download-issues.sh
	bin/download-resources.sh

clean::
	rm -rf ./var

clobber::
	rm -rf var/collection/
	rm -rf dataset/
	
