#!/usr/bin/env python3

import sys
import csv
import glob
import re
import os


fields = "dataset,resource,elapsed,status,exception".split(",")

output_dir = "converted-resource/"
os.makedirs(output_dir, exist_ok=True)

w = csv.DictWriter(open(output_dir + "converted-resource.csv", "w"), fields)
w.writeheader()

for path in glob.glob("var/converted-resource/*/*.csv"):

    m = re.search(r"/([a-zA-Z0-9_-]+)/([a-f0-9]+).csv$", path)
    dataset = m.group(1)
    resource = m.group(2)
    for row in csv.DictReader(open(path, newline="")):
        if row["dataset"] != dataset:
            print(f"Dataet mismatch. dir={dataset} file={row['dataset']}")
        if row["resource"] != resource:
            print(f"Resource mismatch. dir={resource} file={row['resource']}")

        w.writerow(row)
