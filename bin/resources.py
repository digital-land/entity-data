#!/usr/bin/env python3

import sys
import csv

endpoints = {}
resources = {}
pipelines = {}

# TBD: replace with a single SQL query on a collection database ..
for row in csv.DictReader(open("dataset/endpoint.csv", newline="")):
    endpoint = row["endpoint"]
    endpoints[endpoint] = row
    endpoints[endpoint].setdefault("pipelines", {})
    endpoints[endpoint].setdefault("collection", "")

# load sources
for row in csv.DictReader(open("dataset/source.csv", newline="")):
    endpoint = row["endpoint"]

    if endpoint:
        if endpoint not in endpoints:
            print("unknown endpoint", row, file=sys.stderr)
        else:
            # add collection and pipelines to each endpoint
            for pipeline in row["pipelines"].split(";"):
                endpoints[endpoint]["pipelines"][pipeline] = True
                endpoints[endpoint]["collection"] = row["collection"]

# load resources
for row in csv.DictReader(open("dataset/resource.csv", newline="")):
    resource = row["resource"]
    resources[resource] = row
    resources[resource].setdefault("pipelines", {})
    resources[resource]["endpoints"] = row["endpoints"].split(";")

    # add collections to the resource
    for endpoint in resources[resource]["endpoints"]:
        endpoints[endpoint].setdefault("resource", {})
        endpoints[endpoint][resource] = True
        resources[resource]["collection"] = endpoints[endpoint]["collection"]

        # add pipelines to resource
        for pipeline in endpoints[endpoint]["pipelines"]:
            resources[resource]["pipelines"][pipeline] = True

# https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com/{COLLECTION}-collection/issue/{PIPELINE}/{RESOURCE}.csv
for resource in resources:
    collection = resources[resource]["collection"]
    for pipeline in resources[resource]["pipelines"]:
        print(collection, pipeline, resource)
