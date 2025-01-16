#!/bin/sh

set -e

s3="https://files.planning.data.gov.uk/"
expectations_dir="expectations/"
timestamp=`date +%s`

dir=var/$expectations_dir
mkdir -p $dir

csvcut -c dataset,collection specification/dataset.csv | tr ',' ' ' | tail -n +2 |
while read dataset collection
do
    if [ -n "$collection" ] ; then
        echo $dataset $collection

        for file in $dataset-expectation-result.csv $dataset-expectation-issue.csv
        do
            path=$dir$file
            if [ ! -f $path ] ; then
                set -x
                if ! curl -qsfL $flags --retry 3 -o $path $s3$collection-collection/dataset/$file?version=$timestamp ; then
                    echo "*** UNABLE TO DOWNLOAD $collection FROM $path ***"
                fi
                set +x
            fi
        done
    fi
done

mkdir -p expectations
csvstack $dir/*-result.csv > expectations/expectation-result.csv
csvstack $dir/*-issue.csv > expectations/expectation-issue.csv