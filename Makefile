URL=http://datasette-demo.digital-land.info/view_model/
PARAMS=?_labels=on&_stream=on&_size=max
CACHE_DIR=var/cache/

VIEW_MODEL=\
	$(CACHE_DIR)slug.csv\
	$(CACHE_DIR)category.csv\
	$(CACHE_DIR)document.csv\
	$(CACHE_DIR)organisation.csv\
	$(CACHE_DIR)metric.csv\
	$(CACHE_DIR)policy.csv

entity.csv: $(VIEW_MODEL)
	python3 bin/entities.py

$(CACHE_DIR)%.csv:
	@mkdir -p $(CACHE_DIR)
	curl -qfsL '$(URL)$(notdir $@)$(PARAMS)' > $@

download::	$(VIEW_MODEL)

clean::
	rm -rf ./var
