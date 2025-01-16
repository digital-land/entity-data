#!/bin/sh

#set -e

s3="https://files.planning.data.gov.uk/"
timestamp=`date +%s`

python3 bin/resources.py |
while read collection pipeline resource
do
    # https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com/{COLLECTION}-collection/issue/{PIPELINE}/{RESOURCE}.csv
    dir=var/issue/$pipeline
    path=$dir/$resource.csv

    if [ ! -f $path ] ; then
        mkdir -p $dir
        set -x
        curl -qsfL $flags "$s3$collection-collection/issue/$pipeline/$resource.csv?version=$timestamp" > $path
        set +x
    fi
done
