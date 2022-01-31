#!/usr/bin/env python3

import glob
import csv


for path in glob.glob("var/issue/*/*.csv"):
    seen = {}
    for row in csv.DictReader(open(path, newline="")):
        key = row["row-number"] + "," + row["field"]
        if key in seen:
            print(path, row)
        seen[key] = row
