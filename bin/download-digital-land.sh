#!/bin/sh

set -e

s3="https://files.planning.data.gov.uk/"
timestamp=`date +%s`

# Directory to save the database
db_dir="dataset"
db_file="digital-land.sqlite3"
db_path="${db_dir}/${db_file}"
db_url="${s3}digital-land-builder/dataset/${db_file}?version=${timestamp}"

# Create directory if it does not exist
mkdir -p $db_dir

# Download the database only if it does not exist
if [ ! -f "$db_path" ]; then
    echo "Downloading digital-land Database sqlite file"
    set -x
    curl -qsfL --retry 3 -o $db_path $db_url
    set +x
else
    echo "Database file already exists at $db_path"
fi
