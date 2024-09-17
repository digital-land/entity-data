#!/bin/sh

#set -e

s3="https://files.planning.data.gov.uk/"
operational_issue_dir="performance/operational_issue"

mkdir -p $operational_issue_dir

csvcut -c dataset specification/dataset.csv | tr ',' ' ' | tail -n +2 |
while read dataset
do
    path=$operational_issue_dir/$dataset/
    if [ ! -f $path ] ; then
        mkdir -p $dir
        set -x
        echo "$s3$operational_issue_dir/$dataset/operational-issue.csv"
        echo $path
        # curl -qsfL $flags "$s3$operational_issue_dir/$dataset/operational-issue.csv" > $path
        set +x
    fi
done
