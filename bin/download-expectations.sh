#!/bin/sh

set -e

s3="https://files.planning.data.gov.uk/"
expectations_dir="expectations/"

dir=var/$expectations_dir
mkdir -p $dir

csvcut -c dataset,collection specification/dataset.csv | tr ',' ' ' | tail -n +2 |
while read dataset collection
do
    if [ -n "$collection" ] ; then
        echo $dataset $collection

        for file in $dataset-expectation-response.csv $dataset-expectation-issue.csv
        do
            path=$dir$file
            if [ ! -f $path ] ; then
                set -x
                curl -qsfL $flags --retry 3 -o $path $s3$collection-collection/dataset/$file || echo "*** UNABLE TO DOWNLOAD $path ***"
                set +x
            fi
        done
    fi
done

mkdir -p expectations
csvstack $dir/*-response.csv > expectations/expectation-result.csv
csvstack $dir/*-issue.csv > expectations/expectation-issue.csv
