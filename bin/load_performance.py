#!/usr/bin/env python3

# create a database for Performace related metrics

import os
import sys
import csv
import logging
import sqlite3
from digital_land.package.sqlite import SqlitePackage


indexes = {
    "organisation_dataset_summary": ["organisation"]
}


def fetch_data_from_digital_land(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
            SELECT
            p.organisation,
            o.name,
            p.dataset,
            rle.endpoint,
            rle.resource,
            rle.exception,
            rle.status as http_status,
            COUNT(
            CASE
                WHEN it.severity = 'error' THEN 1
                ELSE NULL
            END
        ) as count_error,
            COUNT(
            CASE
                WHEN it.severity = 'warning' THEN 1
                ELSE NULL
            END
        ) as count_warning

        FROM
            provision p
        LEFT JOIN
            organisation o ON o.organisation = p.organisation
        LEFT JOIN
            reporting_latest_endpoints rle
            ON REPLACE(rle.organisation, '-eng', '') = p.organisation
            AND rle.pipeline = p.dataset
        LEFT JOIN
            issue i ON rle.resource = i.resource AND rle.pipeline = i.dataset
        LEFT JOIN
            issue_type it ON i.issue_type = it.issue_type AND it.severity != 'info'
        GROUP BY
            p.organisation,
            p.dataset,
            o.name,
            rle.endpoint
        ORDER BY
            p.organisation,
            o.name;
    """)
    data = cursor.fetchall()
    conn.close()
    return data

def create_organisation_dataset_summary(data, performance_db_path):
    conn = sqlite3.connect(performance_db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS organisation_dataset_summary (
            organisation TEXT,
            name TEXT,
            dataset TEXT,
            endpoint TEXT,
            resource TEXT,
            exception TEXT,
            http_status TEXT,
            count_error INT,
            count_warning INT
        )
    """)
    
    cursor.executemany("""
        INSERT INTO organisation_dataset_summary (organisation, name, dataset, endpoint, resource, exception, http_status, count_error, count_warning)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    level = logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    
    performance_db_path = sys.argv[1] if len(sys.argv) > 1 else "dataset/performance.sqlite3"
    digital_land_db_path = sys.argv[2] if len(sys.argv) > 2 else "dataset/digital-land.sqlite3"
    
    # Fetch data from digital-land database
    data = fetch_data_from_digital_land(digital_land_db_path)
    
    # Create new table and insert data in performance database
    create_organisation_dataset_summary(data, performance_db_path)

    logging.info("New table 'organisation_dataset_summary' created successfully in performance database.")