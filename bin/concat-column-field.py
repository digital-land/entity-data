#!/usr/bin/env python3

import sys
import csv
import glob
import re
import os

fields = ["dataset", "resource", "column", "field",  "entry-date", "end-date", "start-date"]

column_field_dir = "column-field/"
os.makedirs(column_field_dir, exist_ok=True)

w = csv.DictWriter(open(column_field_dir + "column-field.csv", "w"), fields)
w.writeheader()

for path in glob.glob("var/column-field/*/*.csv"):

    m = re.search(r"/([a-zA-Z0-9_-]+)/([a-f0-9]+).csv$", path)
    pipeline = m.group(1)
    resource = m.group(2)
    for row in csv.DictReader(open(path, newline="")):
        row["resource"] = resource
        row["dataset"] = pipeline
        w.writerow(row)
