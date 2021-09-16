#!/bin/sh

python3 bin/concat-source.py | python3 bin/fixdates.py dataset/source.csv

for file in endpoint.csv log.csv resource.csv
do
    csvstack var/collection/*/$file | python3 bin/fixdates.py dataset/$file
done

for file in column.csv concat.csv convert.csv default.csv patch.csv skip.csv transform.csv
do
    csvstack var/pipeline/*/$file | python3 bin/fixdates.py dataset/$file
done

