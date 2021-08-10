#!/bin/sh

for file in endpoint.csv source.csv log.csv resource.csv
do
    csvstack var/collection/*/$file > dataset/$file
done

for file in column.csv concat.csv convert.csv default.csv patch.csv plugins.py skip.csv transform.csv
do
    csvstack var/pipeline/*/$file > dataset/$file
done

