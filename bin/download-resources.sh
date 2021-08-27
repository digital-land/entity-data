#!/bin/sh

set -e

s3="https://collection-dataset.s3.eu-west-2.amazonaws.com/"

python3 bin/resources.py |
while read collection pipeline resource
do
    # https://collection-dataset.s3.eu-west-2.amazonaws.com/{COLLECTION}-collection/resource/{RESOURCE}
    dir=var/resource
    path=$dir/$resource

    if [ ! -f $path ] ; then
        mkdir -p $dir
        set -x
        curl -qsfL $flags "$s3$collection-collection/collection/resource/$resource" > $path
        set +x
    fi

    # python3 bin/filetype.py $path
done