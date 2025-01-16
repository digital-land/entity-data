#!/bin/sh

#set -e

s3="https://files.planning.data.gov.uk/"
operational_issue_dir="performance/operational_issue/"
timestamp=`date +%s`

mkdir -p $operational_issue_dir

csvcut -c dataset specification/dataset.csv | tr ',' ' ' | tail -n +2 |
while read dataset
do
    dir=$operational_issue_dir$dataset
    path=$dir/operational-issue.csv
    if [ ! -f $path ] ; then
        mkdir -p $dir
        set -x
        curl -qsfL $flags "$s3$operational_issue_dir$dataset/operational-issue.csv?version=$timestamp" > $path
        set +x
    fi
done
