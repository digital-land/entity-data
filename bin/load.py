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
# - add refereces for fields such as parent-field
tables = {
    "organisation": "var/cache",
    "collection": "specification",
    "typology": "specification",
    "dataset": "specification",
    "dataset-schema": "specification",
    "datatype": "specification",
    "field": "specification",
    "pipeline": "specification",
    "schema": "specification",
    "schema-field": "specification",
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

    def create_table(self, table, fields, key_field=None, field_datatype={}, unique=None):
        self.execute(
            "CREATE TABLE %s (%s%s%s)"
            % (
                colname(table),
                ",\n".join(
                    [
                        "%s %s%s"
                        % (
                            colname(field),
                            coltype(field_datatype[field]),
                            (" PRIMARY KEY" if field == key_field else ""),
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
                        for field in fields
                        if field in tables and field != table
                    ],
                ),
                "" if not unique else ", UNIQUE(%s)" % (",".join(unique))
            )
        )

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

    def insert(self, table, fields, row):
        self.execute("""
            INSERT OR REPLACE INTO %s(%s)
            VALUES (%s);
            """ % (
            colname(table),
            ",".join([colname(field) for field in fields]),
            ",".join(
                ['"%s"' % row.get(field, "").replace('"', '""') for field in fields]
            ),
        ))

    def load(self, path, table, fields):
        print("loading %s from %s" % (table, path))
        for row in csv.DictReader(open(path, newline="")):
            for field in row:
                if row.get(field, None) is None:
                    row[field] = ""
            self.insert(table, fields, row)

    def load_join(self, path, table, fields, split_field=None, field=None):
        print("loading %s from %s" % (table, path))
        for row in csv.DictReader(open(path, newline="")):
            for value in row[split_field].split(";"):
                row[field] = value
                self.insert(table, fields, row)

    def index(self, table, fields, name=None):
        if not name:
            name = table + "_index"
        print("creating index %s" % (name))
        cols = [colname(field) for field in fields]
        self.execute(
            "CREATE INDEX IF NOT EXISTS %s on %s (%s);" % (name, table, ", ".join(cols))
        )


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

        # make a many-to-many table for each list
        joins = {}
        for field in fields:
            if (
                specification.field[field]["cardinality"] == "n"
                and "%s|%s"
                % (
                    table,
                    field,
                )
                not in ["concat|fields", "convert|parameters", "endpoint|parameters"]
            ):
                parent_field = specification.field[field]["parent-field"]
                joins[field] = parent_field
                field_datatype[parent_field] = specification.field[parent_field]["datatype"]
                fields.remove(field)

        model.create_cursor()
        model.create_table(table, fields, key_field, field_datatype)
        model.commit()

        model.create_cursor()
        model.load(path, table, fields)
        model.commit()

        for split_field, field in joins.items():
            join_table = "%s_%s" % (table, field)

            model.create_cursor()
            model.create_table(join_table, [table, field], None, field_datatype, unique=[table, field])
            model.commit()

            model.create_cursor()
            model.load_join(path, join_table, [table, field], split_field=split_field, field=field)
            model.commit()

    model.index("issue", ["resource", "pipeline", "row-number", "field", "issue-type"])

    model.disconnect()
