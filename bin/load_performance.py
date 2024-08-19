#!/usr/bin/env python3

# create a database for Performace related metrics

import os
import sys
import csv
import logging
import sqlite3
from digital_land.package.sqlite import SqlitePackage
import pandas as pd

indexes = {
    "provision_summary": ["organisation","name","dataset"]
}


def fetch_data_from_digital_land(db_path):
    conn = sqlite3.connect(db_path)
    query = """
        select  
        i.issue_type as issue_type, it.severity, it.responsibility, i.dataset, i.resource
        from issue i
        inner join resource r on i.resource = r.resource
        inner join issue_type it on i.issue_type = it.issue_type
        where r.end_date = ''
    """
    df_issue = pd.read_sql_query(query, conn)
    return df_issue

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

def fetch_reporting_data(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = """
        SELECT 
            rle.organisation,
            rle.name,
            rle.collection,
            rle.pipeline,
            rle.endpoint,
            rle.endpoint_url,
            rle.resource,
            rle.status,
            rle.exception,
            rle.endpoint_entry_date,
            rle.endpoint_end_date,
            rle.resource_start_date,
            rle.resource_end_date,
            max(rle.latest_log_entry_date) as latest_log_entry_date
        FROM 
            reporting_historic_endpoints rle
        WHERE 
        rle.endpoint_end_date = ''
        GROUP BY rle.collection, rle.pipeline,rle.endpoint
        order by rle.collection, rle.pipeline
        """
    df_reporting = pd.read_sql_query(query, conn)
    return df_reporting

if __name__ == "__main__":
    level = logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    
    performance_db_path = sys.argv[1] if len(sys.argv) > 1 else "dataset/performance.sqlite3"
    digital_land_db_path = sys.argv[2] if len(sys.argv) > 2 else "dataset/digital-land.sqlite3"
    
    reporting_data = fetch_reporting_data(performance_db_path)
    # Fetch data from digital-land database
    data = fetch_data_from_digital_land(digital_land_db_path)
    print("Columns in reporting_data:", reporting_data.columns)
    print("Columns in data:", data.columns)
    merged_data = pd.merge(reporting_data, data, left_on=["resource", "pipeline"], right_on=["resource", "dataset"], how="left")
    conn = sqlite3.connect(performance_db_path)
    table_name = "merged_dataset_summary"  
    merged_data.to_sql(table_name, conn, if_exists='replace', index=False)

    final_result = merged_data.groupby(['organisation', 'name', 'pipeline']).agg(
        active_endpoint_count=pd.NamedAgg(
            column='endpoint', 
            aggfunc=lambda x: x.nunique() if merged_data.loc[x.index, 'endpoint_end_date'].eq("").any() else 0
        ),
        error_endpoint_count=pd.NamedAgg(
            column='endpoint', 
            aggfunc=lambda x: x.nunique() if (merged_data.loc[x.index, 'endpoint_end_date'].eq("") & merged_data.loc[x.index, 'status'].ne(200)).any() else 0
        ),
        count_error=pd.NamedAgg(
            column='severity', 
            aggfunc=lambda x: (x.eq('error') & merged_data.loc[x.index, 'endpoint_end_date'].eq("")).sum()
        ),
        count_warning=pd.NamedAgg(
            column='severity', 
            aggfunc=lambda x: (x.eq('warning') & merged_data.loc[x.index, 'endpoint_end_date'].eq("")).sum()
        )
    ).reset_index()


    print("Final result DataFrame:")
    print(final_result.head())


    conn = sqlite3.connect(performance_db_path)
    table_name = "final_dataset_summary"
    final_result.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()


    
    # Create new table and insert data in performance database
    #create_organisation_dataset_summary(merged_data,  performance_db_path)

    logging.info("New table 'provision_summary' created successfully in performance database")