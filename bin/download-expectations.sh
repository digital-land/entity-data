#!/bin/sh

set -e

s3="https://files.planning.data.gov.uk/"
expectations_dir="expectations/"

csvcut -c collection specification/collection.csv | tail -n +2 |
while read collection
do
    for checkpoint in dataset
    do
        for file in dataset/$collection-responses.csv dataset/$collection-issues.csv
        do
            dir=var/$expectations_dir
            path=var/$expectations_dir$file
            if [ ! -f $path ] ; then
                mkdir -p $dir/dataset
                set -x
                curl -qsfL $flags --retry 3 -o $path $s3$expectations_dir$file
                set +x
            fi
        done
    done
done

