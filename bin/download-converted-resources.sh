#!/bin/sh
exit 0;


set -e

s3="https://files.planning.data.gov.uk/"
dir=var/converted-resource

python3 bin/resources.py |
while read collection pipeline resource
do
    path=$dir/$pipeline/$resource.csv

    echo collection: $collection pipeline: $pipeline resource: $resource

    if [ ! -f $path ] ; then
        mkdir -p $dir/$pipeline
        set -x
        curl -qsfL $flags "$s3$collection-collection/$dir/$pipeline/$resource.csv" -o $path ||:
        set +x
    fi
done