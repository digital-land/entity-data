#!/bin/sh

set -e

s3="https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com/"

csvcut -c collection specification/collection.csv | tail -n +2 |
while read collection
do
    for file in endpoint.csv source.csv log.csv resource.csv old-resource.csv
    do
        dir=var/collection/$collection
        path=var/collection/$collection/$file
        if [ ! -f $path ] ; then
            mkdir -p $dir
            set -x
            curl -qsfL $flags --retry 3 -o $path "$s3$collection-collection/collection/$file"
            set +x
        fi
    done
done

