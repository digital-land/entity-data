#!/usr/bin/env python3

# create a database for Performace related metrics

import os
import sys
import csv
import logging
import sqlite3
from digital_land.package.sqlite import SqlitePackage


indexes = {
    "provision_summary": ["organisation","name","dataset"]
}


def fetch_data_from_digital_land(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
                WITH latest_log_entries AS (
        SELECT
            rle.endpoint,
            MAX(rle.latest_log_entry_date) AS latest_log_entry_date
        FROM
            reporting_historic_endpoints rle
        GROUP BY
            rle.endpoint
    ),
    rle_with_latest AS (
        SELECT
            rle.*,
            lle.latest_log_entry_date AS actual_latest_log_entry_date
        FROM
            reporting_historic_endpoints rle
        JOIN
            latest_log_entries lle
        ON
            rle.endpoint = lle.endpoint
        AND rle.latest_log_entry_date = lle.latest_log_entry_date
    )
    SELECT
        p.organisation,
        o.name,
        p.dataset,
        COUNT(DISTINCT CASE WHEN rle.endpoint_end_date IS "" THEN rle.endpoint ELSE NULL END) AS active_endpoint_count,
        COUNT(DISTINCT CASE 
            WHEN rle.endpoint_end_date IS "" AND rle.status != 200 THEN rle.endpoint
            ELSE NULL
        END) AS error_endpoint_count,
        COUNT(
            CASE
                WHEN it.severity = 'error' AND rle.endpoint_end_date IS "" THEN 1
                ELSE NULL
            END
        ) AS count_error,
        COUNT(
            CASE
                WHEN it.severity = 'warning' AND rle.endpoint_end_date IS "" THEN 1
                ELSE NULL
            END
        ) AS count_warning
    FROM
        provision p
        LEFT JOIN organisation o ON o.organisation = p.organisation
        LEFT JOIN rle_with_latest rle
            ON REPLACE(rle.organisation, '-eng', '') = p.organisation
            AND rle.pipeline = p.dataset
        LEFT JOIN issue i 
            ON rle.resource = i.resource AND rle.pipeline = i.dataset
        LEFT JOIN issue_type it 
            ON i.issue_type = it.issue_type AND it.severity != 'info'
    GROUP BY
        p.organisation,
        o.name,
        p.dataset
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
        CREATE TABLE IF NOT EXISTS provision_summary (
            organisation TEXT,
            name TEXT,
            dataset TEXT,
            active_endpoint_count INT,
            error_endpoint_count INT,
            count_error INT,
            count_warning INT
        )
    """)
    
    cursor.executemany("""
        INSERT INTO provision_summary (organisation, name, dataset, active_endpoint_count, error_endpoint_count, count_error, count_warning)
        VALUES (?, ?, ?, ?, ?, ?, ?)
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

    logging.info("New table 'provision_summary' created successfully in performance database")