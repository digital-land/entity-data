#!/usr/bin/env python3

# create a database for exploring collection and pipeline logs

import os
import sys
import csv
import logging
from digital_land.package.sqlite import SqlitePackage


tables = {
    "organisation": "var/cache",
    "collection": "specification",
    "typology": "specification",
    "dataset": "specification",
    "dataset-schema": "specification",
    "datatype": "specification",
    "theme": "specification",
    "field": "specification",
    "pipeline": "specification",
    "schema": "specification",
    "schema-field": "specification",
    "column": "dataset",
    "concat": "dataset",
    "convert": "dataset",
    "default": "dataset",
    "endpoint": "dataset",
    "patch": "dataset",
    "resource": "dataset",
    "skip": "dataset",
    "source": "dataset",
    "transform": "dataset",
    "log": "dataset",
    "filter": "dataset",
    "lookup": "dataset",
}

indexes = {
    "source": ["endpoint"],
    "log": ["endpoint"],
    "resource_endpoint": ["endpoint", "resource"],
}


if __name__ == "__main__":
    level = logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    path = sys.argv[1] if len(sys.argv) > 1 else "dataset/digital-land.sqlite3"
    package = SqlitePackage("digital-land", tables=tables, indexes=indexes)
    package.create(path)
