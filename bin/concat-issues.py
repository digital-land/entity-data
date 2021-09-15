#!/usr/bin/env python3

import sys
import csv
import glob
import re

fields = ["resource", "pipeline", "row-number", "field", "issue-type", "value"]

w = csv.DictWriter(open("dataset/issue.csv", "w"), fields)
w.writeheader()

for path in glob.glob('var/issue/*/*.csv'):

    m = re.search(r'/([a-zA-Z0-9_-]+)/([a-f0-9]+).csv$', path)
    pipeline = m.group(1)
    resource = m.group(2)

    for row in csv.DictReader(open(path, newline="")):
        row["resource"] = resource
        row["pipeline"] = pipeline
        w.writerow(row)
