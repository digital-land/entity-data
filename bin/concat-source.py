#!/usr/bin/env python3

# assemble source.csv with with a constructed source key

import sys
import csv
import glob
import hashlib

fields = [
    "source",
    "attribution",
    "collection",
    "documentation-url",
    "endpoint",
    "licence",
    "organisation",
    "pipelines",
    "entry-date",
    "start-date",
    "end-date",
]

w = csv.DictWriter(sys.stdout, fields)
w.writeheader()

for path in glob.glob("var/collection/*/source.csv"):

    for row in csv.DictReader(open(path, newline="")):
        key = "%s|%s|%s" % (row["collection"], row["organisation"], row["endpoint"] )
        row["source"] = hashlib.md5(key.encode()).hexdigest()
        w.writerow(row)
