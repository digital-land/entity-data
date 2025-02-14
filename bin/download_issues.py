#!/usr/bin/env python3

# create a database for exploring collection, specification, configuration and logs
import sys
import csv
import logging
import click
from  datetime import datetime

from file_downloader import download_urls

#!/usr/bin/env python3

import sys
import csv

logger =  logging.getLogger("__name__")

endpoints = {}
resources = {}
pipelines = {}

def get_endpoints():
    # TBD: replace with a single SQL query on a collection database ..
    for row in csv.DictReader(open("collection/endpoint.csv", newline="")):
        endpoint = row["endpoint"]
        endpoints[endpoint] = row
        endpoints[endpoint].setdefault("pipelines", {})
        endpoints[endpoint].setdefault("collection", "")

# load sources
    for row in csv.DictReader(open("collection/source.csv", newline="")):
        endpoint = row["endpoint"]

        if endpoint:
            if endpoint not in endpoints:
                print("unknown endpoint", row, file=sys.stderr)
            else:
                # add collection and pipelines to each endpoint
                for pipeline in row["pipelines"].split(";"):
                    endpoints[endpoint]["pipelines"][pipeline] = True
                    endpoints[endpoint]["collection"] = row["collection"]
    return endpoints

def get_old_resources():
    old_resources_map = {}
    # Load old-resource.csv to create a mapping of old to updated resources
    for row in csv.DictReader(open("collection/old-resource.csv", newline="")):
        old_resource = row["old-resource"]
        updated_resource = row["resource"]
        old_resources_map[old_resource] = updated_resource
    
    return old_resources_map

def get_resources(endpoints,old_resources_map):
    # load resources
    for row in csv.DictReader(open("collection/resource.csv", newline="")):
        resource = row["resource"]

    # Skip this resource if it's an old resource
        if resource in old_resources_map:
            continue

        resources[resource] = row
        resources[resource].setdefault("pipelines", {})
        resources[resource]["endpoints"] = row["endpoints"].split(";")

        # add collections to the resource
        for endpoint in resources[resource]["endpoints"]:
            if endpoint in endpoints:
                endpoints[endpoint].setdefault("resource", {})
                endpoints[endpoint][resource] = True
                resources[resource]["collection"] = endpoints[endpoint]["collection"]

                # add pipelines to resource
                for pipeline in endpoints[endpoint]["pipelines"]:
                    resources[resource]["pipelines"][pipeline] = True
        
    return resources


@click.command()
@click.option("--timestamp",default=None)
def download_issues(timestamp=None):
# https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com/{COLLECTION}-collection/issue/{PIPELINE}/{RESOURCE}.csv
    endpoints = get_endpoints()
    old_resources = get_old_resources()
    resources = get_resources(endpoints,old_resources)
    url_map = {}

    for resource in resources:
        collection = resources[resource]["collection"]
        # print(f"the c is : {collection}") 
        for pipeline in resources[resource]["pipelines"]:
            if not pipeline:
                logger.error(f"no pipeline for {resource} in {collection} so cannot download")
            else:
                url = f"https://files.planning.data.gov.uk/{collection}-collection/issue/{pipeline}/{resource}.csv?version={timestamp}"
                output_path = f"var/cacche/{pipeline}/{resource}.csv"
                url_map[url]=output_path


    now = datetime.now()
    timestamp = int(now.replace(minute=0, second=0, microsecond=0).timestamp())

    download_urls(url_map)



if __name__ == "__main__":
    download_issues()