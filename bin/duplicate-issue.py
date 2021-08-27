#!/usr/bin/env python3

import glob
import csv


for path in glob.glob('var/issue/*/*.csv'):
    seen = {}
    for row in csv.DictReader(open(path, newline="")):
        key = row["row-number"] + "," + row["field"]
        if key in seen:
            if key == "2,LastUpdatedDate" and seen[key]["value"] == "2018-21-21" and row["value"] == "2017-12-21":
                print(path, row)
        seen[key] = row
