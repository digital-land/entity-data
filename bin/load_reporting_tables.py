#!/usr/bin/env python3

# create a database for Performace related metrics

import sys
import logging
import sqlite3

tables = {
}

indexes = {
}


def fetch_historic_endpoints_data_from_dl(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
        s.organisation,
        o.name,
        s.collection,
        sp.pipeline,
        l.endpoint,
        e.endpoint_url,
        l.status,
        l.exception,
        l.resource,

        max(l.entry_date) as latest_log_entry_date,
        e.entry_date as endpoint_entry_date,
        e.end_date as endpoint_end_date,
        r.start_date as resource_start_date,
        r.end_date as resource_end_date

    FROM
        log l
        INNER JOIN source s on l.endpoint = s.endpoint
        INNER JOIN endpoint e on l.endpoint = e.endpoint
        INNER JOIN organisation o on o.organisation = replace(s.organisation, '-eng', '')
        INNER JOIN source_pipeline sp on s.source = sp.source
        LEFT JOIN resource r on l.resource = r.resource

    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9

    ORDER BY
        s.organisation, o.name, s.collection, sp.pipeline, latest_log_entry_date DESC
    """)
    data = cursor.fetchall()
    conn.close()
    return data


def fetch_latest_endpoints_data_from_dl(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * 
            from (
            SELECT
                s.organisation,
                o.name,
                s.collection,
                sp.pipeline,
                l.endpoint,
                e.endpoint_url,
                l.status,
                t2.days_since_200,
                l.exception,
                l.resource,

                max(l.entry_date) as latest_log_entry_date,
                e.entry_date as endpoint_entry_date,
                e.end_date as endpoint_end_date,
                r.start_date as resource_start_date,
                r.end_date as resource_end_date,
            
                row_number() over (partition by s.organisation,sp.pipeline order by e.entry_date desc, l.entry_date desc) as rn

            FROM
                log l
                INNER JOIN source s on l.endpoint = s.endpoint
                INNER JOIN endpoint e on l.endpoint = e.endpoint
                INNER JOIN organisation o on o.organisation = replace(s.organisation, '-eng', '')
                INNER JOIN source_pipeline sp on s.source = sp.source
                LEFT JOIN resource r on l.resource = r.resource
                LEFT JOIN (
                    SELECT
                        endpoint,
                        cast(julianday('now') - julianday(max(entry_date)) as int) as days_since_200
                    FROM
                        log
                    WHERE
                        status=200
                    GROUP BY
                        endpoint
                ) t2 on e.endpoint = t2.endpoint

            WHERE
                e.end_date=''
            GROUP BY
                1, 2, 3, 4, 5, 6, 7, 8, 9

            ORDER BY
                s.organisation, o.name, s.collection, sp.pipeline, endpoint_entry_date DESC
            ) t1
        where t1.rn = 1 
    """)
    data = cursor.fetchall()
    conn.close()
    return data


def create_reporting_tables(historic_endpoints_data, latest_endpoint_data, performance_db_path):
    conn = sqlite3.connect(performance_db_path)
    cursor = conn.cursor()
    
    # Create the reporting_historic_endpoints table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reporting_historic_endpoints (
            organisation TEXT,
            name TEXT,
            collection TEXT,
            pipeline TEXT,
            endpoint TEXT,
            endpoint_url TEXT,
            status TEXT,
            exception TEXT,
            resource TEXT,
            latest_log_entry_date TEXT,
            endpoint_entry_date TEXT,
            endpoint_end_date TEXT,
            resource_start_date TEXT,
            resource_end_date TEXT
        )
    """)
    cursor.executemany("""
            INSERT INTO reporting_historic_endpoints (
                organisation, name, collection, pipeline, endpoint, endpoint_url, status, exception, resource, 
                latest_log_entry_date, endpoint_entry_date, endpoint_end_date, resource_start_date, resource_end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, historic_endpoints_data)

     # Create the reporting_latest_endpoints table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reporting_latest_endpoints (
            organisation TEXT,
            name TEXT,
            collection TEXT,
            pipeline TEXT,
            endpoint TEXT,
            endpoint_url TEXT,
            status TEXT,
            days_since_200 INTEGER,
            exception TEXT,
            resource TEXT,
            latest_log_entry_date TEXT,
            endpoint_entry_date TEXT,
            endpoint_end_date TEXT,
            resource_start_date TEXT,
            resource_end_date TEXT,
            rn INTEGER
        )
    """)
    
    # Insert data into reporting_latest_endpoints
    cursor.executemany("""
            INSERT INTO reporting_latest_endpoints (
                organisation, name, collection, pipeline, endpoint, endpoint_url, status, days_since_200, exception, resource, 
                latest_log_entry_date, endpoint_entry_date, endpoint_end_date, resource_start_date, resource_end_date, rn
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
        """, latest_endpoint_data)


    conn.commit()
    conn.close()

if __name__ == "__main__":
    level = logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    
    performance_db_path = sys.argv[1] if len(sys.argv) > 1 else "dataset/performance.sqlite3"
    digital_land_db_path = sys.argv[2] if len(sys.argv) > 2 else "dataset/digital-land.sqlite3"
    
    # Fetch data from digital-land database
    historic_endpoints_data = fetch_historic_endpoints_data_from_dl(digital_land_db_path)
    latest_endpoints_data = fetch_latest_endpoints_data_from_dl(digital_land_db_path)
    
    create_reporting_tables(historic_endpoints_data,latest_endpoints_data,performance_db_path)
