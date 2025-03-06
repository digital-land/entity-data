#!/usr/bin/env python3

import csv


organisations = {}
datasets = {}


for row in csv.DictReader(open("collection/source.csv")):
    organisation = row["organisation"]
    pipelines = row.get("datasets", "") or row.get("pipelines", "")

    for dataset in pipelines.split(";"):
        organisations.setdefault(organisation, set())
        organisations[organisation].add(dataset)

        datasets.setdefault(dataset, set())
        datasets[dataset].add(organisation)


#for organisation in sorted(organisations):
    #if not (organisation.startswith("local-authority") or organisation.startswith("development-corporation") or organisation.startswith("national-park")): 
        #print('"' + organisation + '":', sorted(organisations[organisation]))

for organisation in sorted(organisations):
    if (organisation.startswith("local-authority") or organisation.startswith("development-corporation") or organisation.startswith("national-park")): 
        print(organisation)
