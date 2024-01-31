#!/usr/bin/env python3

# create a database for exploring collection, specification, configuration and logs

import os
import sys
import csv
import logging
import sqlite3
from digital_land.package.sqlite import SqlitePackage


tables = {
    "organisation": "var/cache",
    "organisation-dataset": "specification",

    "source": "collection",
    "endpoint": "collection",
    "resource": "collection",
    "old-resource": "collection",
    "log": "collection",

    "collection": "specification",
    "theme": "specification",
    "typology": "specification",
    "dataset": "specification",
    "dataset-field": "specification",
    "field": "specification",
    "datatype": "specification",
    "prefix": "specification",
    "severity": "specification",
    "issue-type": "specification",
    "attribution": "specification",
    "licence": "specification",
    "project": "specification",
    "project-status": "specification",
    "provision-reason": "specification",
    "specification": "specification",
    "specification-status": "specification",

    "column": "pipeline",
    "combine": "pipeline",
    "concat": "pipeline",
    "convert": "pipeline",
    "default": "pipeline",
    "default-value": "pipeline",
    "patch": "pipeline",
    "skip": "pipeline",
    "transform": "pipeline",
    "filter": "pipeline",
    "lookup": "pipeline",
}

indexes = {
    "source": ["endpoint"],
    "log": ["endpoint", "resource"],
    "resource_dataset": ["resource", "dataset"],
    "resource_endpoint": ["resource", "endpoint"],
    "resource_organisation": ["resource", "organisation"],
    "specification_dataset": ["specification", "dataset"],
    "organisation_dataset": ["organisation", "dataset"],
}


if __name__ == "__main__":
    level = logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    path = sys.argv[1] if len(sys.argv) > 1 else "dataset/digital-land.sqlite3"
    package = SqlitePackage("digital-land", path=path, tables=tables, indexes=indexes)
    package.create()
    
    conn = sqlite3.connect(path)
    conn.execute("""
    CREATE VIEW endpoint_most_recent_log_date as
    SELECT endpoint,max(date(entry_date)) as most_recent_log_date
    FROM log 
    GROUP BY endpoint
    """)

    conn.execute("""
    CREATE VIEW most_recent_logs as
    select t1.*
    from log t1 
    inner join endpoint_most_recent_log_date t2 on t1.endpoint = t2.endpoint
    where date(t1.entry_date) = t2.most_recent_log_date
    """)

    conn.close()
