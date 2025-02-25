#!/usr/bin/env python3

import logging
import click
from datetime import datetime

from bin.resources import get_endpoints, get_old_resources, get_resources
from file_downloader import download_urls

logger =  logging.getLogger("__name__")

@click.command()
@click.option("--timestamp",default=None)
def download_column_field(timestamp=None):
# https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com/{COLLECTION}-collection/issue/{PIPELINE}/{RESOURCE}.csv
    endpoints = get_endpoints()
    old_resources = get_old_resources()
    resources = get_resources(endpoints,old_resources)
    url_map = {}
    now = datetime.now()
    timestamp = int(now.replace(minute=0, second=0, microsecond=0).timestamp())

    for resource in resources:
        collection = resources[resource]["collection"]
        for pipeline in resources[resource]["pipelines"]:
            if not pipeline:
                logger.error(f"no pipeline for {resource} in {collection} so cannot download")
            else:
                url = f"https://files.planning.data.gov.uk/{collection}-collection/var/column-field/{pipeline}/{resource}.csv?version={timestamp}"
                output_path = f"var/column-field/{pipeline}/{resource}.csv"
                url_map[url]=output_path

    download_urls(url_map)



if __name__ == "__main__":
    download_column_field()