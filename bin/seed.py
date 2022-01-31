#!/usr/bin/python3

import sys
import csv

field_size_limit = sys.maxsize

while True:
    try:
        csv.field_size_limit(field_size_limit)
        break
    except OverflowError:
        field_size_limit = int(field_size_limit / 10)


entity_number = 0

entity = {}
slug = {}

if __name__ == "__main__":
    # id,prefix,organisation,reference,name,entry_date,start_date,end_date
    # 1,,development-corporation:Q20648596,,Old Oak and Park Royal Development Corporation,,2015-04-01,
    for row in csv.DictReader(open("var/cache/organisation.csv", newline="")):
        prefix, reference = row["organisation"].split(":")
        slug = "/organisation/" + prefix + "/" + reference
        print(slug)

    # id,slug_id,slug_id_label,category,type,reference,name,entry_date,start_date,end_date
    # 1,1,/document-type/area-appraisal,area-appraisal,document-type,,Area appraisal,2021-04-23,,
    for row in csv.DictReader(open("var/cache/category.csv", newline="")):
        slug = row["slug_id_label"]
        slug = slug.replace("\t", "")
        print(slug)

    # id,slug_id,slug_id_label,geography,geometry,point,name,notes,documentation_url,type,entry_date,start_date,end_date
    # 1,141,/local-authority-district/E06000001,local-authority-district:E06000001,"MULTIPOLYGON ..",,Hartlepool,,,local-authority-district,2021-02-06,,
    for row in csv.DictReader(open("var/cache/geography.csv", newline="")):
        slug = row["slug_id_label"]
        if slug:
            print(slug)

    # load("policy")
    # load("document")
