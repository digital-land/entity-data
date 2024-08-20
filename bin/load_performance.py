#!/usr/bin/env python3

# create a database for Performace related metrics

import sys
import logging
import sqlite3
import pandas as pd

indexes = {
    "provision_summary": ["organisation", "name", "dataset"]
}

def fetch_provision_data(db_path):
    conn = sqlite3.connect(db_path)
    query = """
        select p.organisation, o.name, p.cohort, p.dataset from provision p
        inner join organisation o on o.organisation = p.organisation
        order by p.organisation
    """
    df_provsion = pd.read_sql_query(query, conn)
    return df_provsion

def fetch_issue_data(db_path):
    conn = sqlite3.connect(db_path)
    query = """
        select  
        count(*) as count_issues, strftime('%d-%m-%Y', 'now') as date,
        i.issue_type as issue_type, it.severity, it.responsibility, i.dataset, i.resource
        from issue i
        inner join resource r on i.resource = r.resource
        inner join issue_type it on i.issue_type = it.issue_type
        where r.end_date = ''
        group by i.resource,i.issue_type
    """
    df_issue = pd.read_sql_query(query, conn)
    return df_issue


def create_organisation_dataset_summary(merged_data, performance_db_path):
    conn = sqlite3.connect(performance_db_path)
    table_name = "issue_summary"  
    merged_data.to_sql(table_name, conn, if_exists='replace', index=False)
    final_result = merged_data.groupby(['organisation', 'name', 'pipeline']).agg(
        active_endpoint_count=pd.NamedAgg(
            column='endpoint', 
            aggfunc='nunique'
        ),
         error_endpoint_count=pd.NamedAgg(
            column='endpoint', 
            aggfunc=lambda x: x[merged_data.loc[x.index, 'status'] != '200'].nunique()
        ),
        count_internal_issue=pd.NamedAgg(
            column='count_issues',  
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'error') & 
                                (merged_data.loc[x.index, 'responsibility'] == 'internal')].sum()
        ),
        count_external_issue=pd.NamedAgg(
            column='count_issues',  
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'error') & 
                                (merged_data.loc[x.index, 'responsibility'] == 'external')].sum()
        ),
        count_internal_warning=pd.NamedAgg(
            column='count_issues',  
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'warning') & 
                                (merged_data.loc[x.index, 'responsibility'] == 'internal')].sum()
        ),
        count_external_warning=pd.NamedAgg(
            column='count_issues',  
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'warning') & 
                                (merged_data.loc[x.index, 'responsibility'] == 'external')].sum()
        ),
        count_internal_notice=pd.NamedAgg(
            column='count_issues',  
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'notice') & 
                                (merged_data.loc[x.index, 'responsibility'] == 'internal')].sum()
        ),
        count_external_notice=pd.NamedAgg(
            column='count_issues',  
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'notice') & 
                                (merged_data.loc[x.index, 'responsibility'] == 'external')].sum()
        )
    ).reset_index()

    table_name = "provsion_summary"
    final_result.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    

def fetch_reporting_data(db_path):
    conn = sqlite3.connect(db_path)
    query = """
        SELECT 
            rle.organisation,
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
    
    provision_data = fetch_provision_data(digital_land_db_path)
    issue_data = fetch_issue_data(digital_land_db_path)

    reporting_data = fetch_reporting_data(performance_db_path)

    merged_data = pd.merge(reporting_data, issue_data, left_on=["resource", "pipeline"], right_on=["resource", "dataset"], how="left")
    
    # Create new table and insert data in performance database
    create_organisation_dataset_summary(merged_data,  performance_db_path)

    logging.info(
        "New table 'provision_summary' created successfully in performance database")
