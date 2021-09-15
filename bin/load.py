#!/usr/bin/env python3

# create a database for exploring collection and pipeline logs

import os
import sys
import csv
import sqlite3

debug = True
debug = False

# TBD: 
# - take this config from specification for data-packages
# - make each package a separate database collection, pipeline, log
# - add refereces
# - build many-to-many tables from [n] lists and join tables (schema-field)
tables = {
    "organisation": "var/cache",
    "collection": "specification",
    "dataset": "specification",
    "dataset-schema": "specification",
    "datatype": "specification",
    "field": "specification",
    "pipeline": "specification",
    "schema": "specification",
    "schema-field": "specification",
    "typology": "specification",
    "column": "dataset",
    "concat": "dataset",
    "convert": "dataset",
    "default": "dataset",
    "endpoint": "dataset",
    "patch": "dataset",
    "resource": "dataset",
    "skip": "dataset",
    "source": "dataset",
    "transform": "dataset",
    "log": "dataset",
    "issue": "dataset",
}


class Specification:
    def __init__(self):
        self.schema = {}
        self.field = {}

    def load(self):
        for row in csv.DictReader(open("specification/field.csv", newline="")):
            self.field[row["field"]] = row

        for row in csv.DictReader(open("specification/schema.csv", newline="")):
            self.schema[row["schema"]] = row
            self.schema[row["schema"]].setdefault("fields", [])

        for row in csv.DictReader(open("specification/schema-field.csv", newline="")):
            self.schema[row["schema"]]["fields"].append(row["field"])


def colname(field):
    if field == "default":
        return "_default"
    return field.replace("-", "_")


def coltype(datatype):
    if datatype == "integer":
        return "INTEGER"
    else:
        return "TEXT"


class Model:
    def __init__(self):
        pass

    def connect(self, path):
        self.path = path
        self.connection = sqlite3.connect(path)

    def disconnect(self):
        self.connection.close()

    def create_table(self, table, fields, key_field=None, field_datatype={}):
        self.execute("CREATE TABLE %s (%s%s)" % (
            colname(table),
            ",\n".join(
                [
                    "%s %s%s"
                    % (
                        colname(field),
                        coltype(field_datatype[field]),
                        (" PRIMARY KEY" if field == key_field and field not in ["source"] else "")
                    )
                    for field in fields
                ]
            ),
            "\n".join(
                [
                    ", FOREIGN KEY (%s) REFERENCES %s (%s)"
                    % (
                        colname(field),
                        colname(field),
                        colname(field),
                    )
                    for field in fields if field in tables and field != table
                ]
            ),
        ))


    def create_cursor(self):
        self.cursor = self.connection.cursor()
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA journal_mode = OFF")

    def commit(self):
        if debug:
            print("committing ..")

        self.connection.commit()

    def execute(self, cmd):
        if debug:
            print(cmd)
        self.cursor.execute(cmd)

    def load(self, path, table, fields):
        print("loading %s from %s" % (table, path))
        for row in csv.DictReader(open(path, newline="")):
            cmd = """
                INSERT OR REPLACE INTO %s(%s)
                VALUES (%s);
                """ % (
                colname(table),
                ",".join([colname(field) for field in fields]),
                ",".join(['"%s"' % row.get(field, "").replace('"', '""') for field in fields]),
            )

            self.execute(cmd)

    def index(self, table, fields, name=None):
        if not name:
            name = table + "_index"
        print("creating index %s" % (name))
        cols = [colname(field) for field in fields]
        self.execute("CREATE INDEX IF NOT EXISTS %s on %s (%s);" % (name, table, ", ".join(cols)))


if __name__ == "__main__":

    specification = Specification()
    specification.load()

    model = Model()

    db = sys.argv[1] if len(sys.argv) > 1 else "digital-land.db"

    if os.path.exists(db):
        os.remove(db)

    model.connect(db)

    for table in tables:
        path = "%s/%s.csv" % (tables[table], table)
        fields = specification.schema[table]["fields"]
        key_field = specification.schema[table]["key-field"] or table
        field_datatype = {field: specification.field["datatype"] for field in fields}

        model.create_cursor()
        model.create_table(table, fields, key_field, field_datatype)
        model.commit()

        model.create_cursor()
        model.load(path, table, fields)
        model.commit()

    model.index("issue", ["resource", "pipeline", "row-number", "issue-type"])

    model.disconnect()
