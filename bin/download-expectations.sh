#!/bin/sh

set -e

s3="https://files.planning.data.gov.uk/"
aws_s3_bucket="s3://development-collection-data/" 
expectations_dir="expectations/"

dir=var/$expectations_dir
mkdir -p $dir

get_headers() {
    case $1 in
        endpoint.csv)
            echo "endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date"
            ;;
        source.csv)
            echo "source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date"
            ;;
        log.csv)
            echo "bytes,content-type,elapsed,endpoint,resource,status,entry-date,start-date,end-date,exception"
            ;;
        resource.csv)
            echo "resource,bytes,organisations,datasets,endpoints,start-date,end-date"
            ;;
        old-resource.csv)
            echo "old-resource,status,resource"
            ;;
        *)
            echo ""
            ;;
    esac
}

csvcut -c collection specification/collection.csv | tail -n +2 |
while read collection
do
    for file in endpoint.csv source.csv log.csv resource.csv old-resource.csv
    do
        dir=var/collection/$collection
        path=var/collection/$collection/$file
        if [ ! -f $path ] ; then
            mkdir -p $dir
            set -x
            if ! curl -qsfL --retry 3 -o $path "$s3$collection-collection/collection/$file"; then
                # If curl fails, create a new file with the appropriate headers
                headers=$(get_headers $file)
                echo "$headers" > $path
                # Upload the newly created file to the S3 bucket
                aws s3 cp $path $aws_s3_bucket$collection-collection/collection/$file --no-progress
                echo "File $file created and uploaded to S3: $aws_s3_bucket$collection-collection/collection/$file"
            fi
            set +x
        fi
    done
done

mkdir -p expectations
csvstack $dir/*-result.csv > expectations/expectation-result.csv
csvstack $dir/*-issue.csv > expectations/expectation-issue.csv