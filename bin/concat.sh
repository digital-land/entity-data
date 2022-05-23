#!/bin/sh

set -e

mkdir -p collection/
for table in source endpoint log resource old-resource
do
    file=$table.csv
    set -x
    python3 bin/csvcat.py $table collection/$file var/collection/*/$file
    set +x
done

mkdir -p pipeline/
for table in column combine concat convert default default-value patch skip transform filter lookup
do
    file=$table.csv
    set -x
    python3 bin/csvcat.py $table pipeline/$file $(find var/pipeline/ -name $file -size +0)
    set +x
done

