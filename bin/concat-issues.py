#!/usr/bin/env python3

import sys
import csv
import glob
import re
import os

fields = ["resource", "pipeline", "row-number", "field", "issue-type", "value", "message", "dataset", "entry-number", "line-number", "entity"]

issues_dir = "issues/"
os.makedirs(issues_dir, exist_ok=True)

w = csv.DictWriter(open(issues_dir + "issue.csv", "w"), fields)
w.writeheader()

for path in glob.glob("var/issue/*/*.csv"):

    m = re.search(r"/([a-zA-Z0-9_-]+)/([a-f0-9]+).csv$", path)
    pipeline = m.group(1)
    resource = m.group(2)

    for row in csv.DictReader(open(path, newline="")):
        row["resource"] = resource
        row["pipeline"] = pipeline
        w.writerow(row)

operational_issue_dir = "performance/operational_issue/"
fields = ["dataset","resource","line-number","entry-number","field","issue-type","value","message","entry-date"]
w = csv.DictWriter(open(os.path.join(operational_issue_dir,"operational-issue.csv"), "w"), fields)
w.writeheader()

for path in glob.glob("performance/operational_issue/*/operational-issue.csv"):
    for row in csv.DictReader(open(path, newline="")):
        w.writerow(row)
