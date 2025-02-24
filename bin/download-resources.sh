#!/bin/sh

set -e

s3="https://files.planning.data.gov.uk/"
timestamp=`date +%s`

python3 bin/resources.py |
while read collection pipeline resource
do
    # https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com/{COLLECTION}-collection/resource/{RESOURCE}
    dir=var/resource
    path=$dir/$resource

    if [ ! -f $path ] ; then
        mkdir -p $dir
        set -x
        curl -qsfL $flags "$s3$collection-collection/collection/resource/$resource?version=$timestamp" > $path
        set +x
    fi
done
