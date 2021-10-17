#!/bin/sh

set -e

github="https://raw.githubusercontent.com/digital-land/"

csvcut -c collection specification/collection.csv | tail -n +2 |
while read collection
do
    for file in endpoint.csv source.csv log.csv resource.csv
    do
        dir=var/collection/$collection
        path=var/collection/$collection/$file
        if [ ! -f $path ] ; then
            mkdir -p $dir
            set -x
            curl -qsfL $flags -o $path "$github$collection-collection/main/collection/$file"
            set +x
        fi
    done
done

