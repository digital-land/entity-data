#!/usr/bin/env python3

import sys
import csv
from datetime import datetime

w = None
fields = None

print(sys.argv[1])


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


for row in csv.DictReader(sys.stdin):
    if not w:
        fields = row.keys()
        w = csv.DictWriter(open(sys.argv[1], "w"), fields)
        w.writeheader()

    for col in row:
        if col == "entry-date":
            row[col] = as_timestamp(row[col])
        elif col.endswith("-date"):
            row[col] = as_date(row[col])

    w.writerow(row)
