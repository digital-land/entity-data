#!/bin/sh

s3="https://files.planning.data.gov.uk/"
column_field_dir="column-field/"

python3 bin/resources.py |
while read collection pipeline resource
do
    dir=var/column-field/$pipeline
    path=$dir/$resource.csv

    echo collection: $collection pipeline: $pipeline resource: $resource

    if [ ! -f $path ] ; then
        mkdir -p $dir
        set -x
        curl -qsfL $flags "$s3$collection-collection/var/column-field/$pipeline/$resource.csv" > $path
        set +x
    fi
done