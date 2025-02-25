#!/usr/bin/env python3
import logging
import click
from datetime import datetime

from bin.resources import get_resources
from file_downloader import download_urls


logger =  logging.getLogger("__name__")

@click.command()
@click.option("--timestamp",default=None)
def download_issues(timestamp=None):
    resources = get_resources("collection/")
    url_map = {}
    now = datetime.now()
    timestamp = int(now.replace(minute=0, second=0, microsecond=0).timestamp())

    for resource in resources:
        collection = resources[resource]["collection"]
        for pipeline in resources[resource]["pipelines"]:
            if not pipeline:
                logger.error(f"no pipeline for {resource} in {collection} so cannot download")
            else:
                url = f"https://files.planning.data.gov.uk/{collection}-collection/issue/{pipeline}/{resource}.csv?version={timestamp}"
                output_path = f"var/issue/{pipeline}/{resource}.csv"
                url_map[url]=output_path
    download_urls(url_map)



if __name__ == "__main__":
    download_issues()