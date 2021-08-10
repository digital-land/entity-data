#!/bin/sh

set -e

github="https://raw.githubusercontent.com/digital-land/"
datasette="http://datasette-demo.digital-land.info/"

download () {
    db=$1
    collection=$2
    file=$3
    flags=$4
    dir=var/$db/$collection
    path=var/$db/$collection/$file
    if [ ! -f $path ] ; then
        mkdir -p $dir
        set -x
        curl -qsL $flags "$github$collection-collection/main/$db/$file" > $path
        set +x
    fi
}

csvcut -c collection ../specification/specification/collection.csv | tail -n +2 |
while read collection
do
    # collection files
    for file in endpoint.csv source.csv log.csv resource.csv
    do
        download collection $collection $file -f
    done

    # pipeline files
    for file in column.csv concat.csv convert.csv default.csv patch.csv plugins.py skip.csv transform.csv
    do
        download pipeline $collection $file
    done
done

