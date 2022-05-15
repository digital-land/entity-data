#!/usr/bin/env python3

import sys
import csv
import hashlib
from datetime import datetime

dataset = sys.argv[1]
output_path = sys.argv[2]
paths = sys.argv[3:]


def as_timestamp(date):
    if not date:
        return ""
    if len(date) >= 20:
        dt = datetime.strptime(date[:19], "%Y-%m-%dT%H:%M:%S")
    elif len(date) == 20:
        dt = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
    elif len(date) == 10:
        dt = datetime.strptime(date, "%Y-%m-%d")
    elif len(date) == 7:
        dt = datetime.strptime(date, "%Y-%m")
    elif len(date) == 4:
        dt = datetime.strptime(date, "%Y")
    else:
        print("unknown date format", date)
        sys.exit(2)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def as_date(date):
    if not date:
        return ""
    if len(date) == 10:
        dt = datetime.strptime(date, "%Y-%m-%d")
    elif len(date) == 7:
        dt = datetime.strptime(date, "%Y-%m")
    elif len(date) == 4:
        dt = datetime.strptime(date, "%Y")
    else:
        print("unknown date format", date)
        sys.exit(2)

    return dt.strftime("%Y-%m-%d")


fieldnames = []
for row in csv.DictReader(open("specification/schema-field.csv", "r", newline="")):
    if row["schema"] == dataset:
        fieldnames.append(row["field"])


writer = csv.DictWriter(open(output_path, "w", newline=""), fieldnames=fieldnames, extrasaction="ignore")
writer.writeheader()

for path in paths:
    for row in csv.DictReader(open(path, "r", newline="")):

        # default the source field
        if dataset == "source" and row.get("source", ""):
            key = "%s|%s|%s" % (row["collection"], row["organisation"], row["endpoint"])
            row["source"] = hashlib.md5(key.encode()).hexdigest()

        # migrate piperow to dataset
        if "dataset" in fieldnames and not row.get("dataset", ""):
            row["dataset"] = row.get("pipeline", "")

        # fix dates
        for col in row:
            if not col:
                print(row)
            if col == "entry-date":
                row[col] = as_timestamp(row[col])
            elif col.endswith("-date"):
                row[col] = as_date(row[col])

        writer.writerow(row)
