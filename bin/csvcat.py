#!/usr/bin/env python3

import sys
import csv

# csvstack can't handle files with differt columns ..

paths = sys.argv[1:]
fieldnames = []

for path in paths:
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)
        for h in headers:
            if h not in fieldnames:
                fieldnames.append(h)

writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
writer.writeheader()
for path in paths:
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            writer.writerow(line)
