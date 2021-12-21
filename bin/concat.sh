#!/bin/sh

python3 bin/concat-source.py | python3 bin/fixdates.py dataset/source.csv
for file in endpoint.csv log.csv resource.csv
do
    python3 bin/csvcat.py var/collection/*/$file | python3 bin/fixdates.py dataset/$file
done

for file in column.csv concat.csv convert.csv default.csv patch.csv skip.csv transform.csv filter.csv lookup.csv
do
    set -x
    python3 bin/csvcat.py $(find var/pipeline/ -name $file -size +0) | python3 bin/fixdates.py dataset/$file
    set +x
done

