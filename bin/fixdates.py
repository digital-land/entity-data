#!/usr/bin/env python3

import sys
import csv

w = None
fields = None

print(sys.argv[1])

for row in csv.DictReader(sys.stdin):
    if not w:
        fields = row.keys()
        w = csv.DictWriter(open(sys.argv[1], "w"), fields)
        w.writeheader()

    for col in row:
        if col.endswith("-date"):
            s = row[col]
            if len(s) > 10:
                s = s[:10]
            elif len(s) == 7:
                s = s + "-01"
            elif len(s) == 4:
                s = s + "-01-01"
            row[col] = s

    w.writerow(row)
