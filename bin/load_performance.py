#!/usr/bin/env python3

# create a database for Performace related metrics

import sys
import logging
import sqlite3
import pandas as pd

indexes = {
    "provision_summary": ["organisation", "organisation_name", "dataset"]
}


def fetch_provision_data(db_path):
    conn = sqlite3.connect(db_path)
    query = """
        select p.organisation, o.name as organisation_name, p.cohort, p.dataset from provision p
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
        i.issue_type as issue_type, it.severity, it.responsibility, i.dataset, i.resource, GROUP_CONCAT(DISTINCT i.field) as fields
        from issue i
        inner join resource r on i.resource = r.resource
        inner join issue_type it on i.issue_type = it.issue_type
        where r.end_date = ''
        group by i.dataset,i.resource,i.issue_type
    """
    df_issue = pd.read_sql_query(query, conn)
    return df_issue


def fetch_column_field_data(db_path):
    conn = sqlite3.connect(db_path)
    query = """
        select
        cf.resource,
        cf.dataset,
        GROUP_CONCAT(
           DISTINCT CASE
                WHEN UPPER(cf.field) = UPPER(REPLACE(REPLACE(cf.column, " ", "-"), "_", "-"))
                or cf.field in ('geometry', 'point') THEN cf.field
                ELSE NULL
            END, ';') as mapping_field,
        GROUP_CONCAT(
           DISTINCT CASE
                WHEN UPPER(cf.field) != UPPER(REPLACE(REPLACE(cf.column, " ", "-"), "_", "-"))
                and cf.field not in ('geometry', 'point') THEN cf.field
                WHEN cf.field in ('geometry', 'point') THEN null
                ELSE NULL
            END, ';') as non_mapping_field
            
        from
            column_field cf
            inner join resource r on cf.resource = r.resource
        where
            r.end_date = ''
        group by
            cf.resource,
            cf.dataset
    """

    df_column_field = pd.read_sql_query(query, conn)
    return df_column_field


def create_performance_tables(merged_data, cf_merged_data, performance_db_path):
    conn = sqlite3.connect(performance_db_path)

    column_field_table_name = "endpoint_dataset_resource_summary"
    cf_merged_data_filtered = cf_merged_data[cf_merged_data['resource'].notna(
    )]
    cf_merged_data_filtered = cf_merged_data_filtered.drop(['latest_status', 'exception', 'endpoint_entry_date', 'endpoint_end_date'], axis=1, errors='ignore')
    cf_merged_data_filtered.to_sql(
        column_field_table_name, conn, if_exists="replace", index=False)

    issue_table_name = "endpoint_dataset_issue_type_summary"
    issue_data_filtered = merged_data[merged_data['resource'].notna()]
    issue_data_filtered = issue_data_filtered.drop(['latest_status', 'exception', 'endpoint_entry_date', 'endpoint_end_date'], axis=1, errors='ignore')
    issue_data_filtered.to_sql(issue_table_name, conn, if_exists='replace', index=False, dtype={
                               'count_issues': 'INTEGER'})

    final_result = merged_data.groupby(['organisation', 'organisation_name', 'dataset']).agg(
        active_endpoint_count=pd.NamedAgg(
            column='endpoint',
            aggfunc='nunique'
        ),
         error_endpoint_count=pd.NamedAgg(
            column='endpoint', 
            aggfunc=lambda x: x[merged_data.loc[x.index, 'latest_status'] != '200'].nunique()
        ),
        count_issue_error_internal=pd.NamedAgg(
            column='count_issues',
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'error') &
                                (merged_data.loc[x.index, 'responsibility'] == 'internal')].sum()
        ),
        count_issue_error_external=pd.NamedAgg(
            column='count_issues',
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'error') &
                                (merged_data.loc[x.index, 'responsibility'] == 'external')].sum()
        ),
        count_issue_warning_internal=pd.NamedAgg(
            column='count_issues',
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'warning') &
                                (merged_data.loc[x.index, 'responsibility'] == 'internal')].sum()
        ),
        count_issue_warning_external=pd.NamedAgg(
            column='count_issues',
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'warning') &
                                (merged_data.loc[x.index, 'responsibility'] == 'external')].sum()
        ),
        count_issue_notice_internal=pd.NamedAgg(
            column='count_issues',
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'notice') &
                                (merged_data.loc[x.index, 'responsibility'] == 'internal')].sum()
        ),
        count_issue_notice_external=pd.NamedAgg(
            column='count_issues',
            aggfunc=lambda x: x[(merged_data.loc[x.index, 'severity'] == 'notice') &
                                (merged_data.loc[x.index, 'responsibility'] == 'external')].sum()
        )
    ).reset_index()

    # Convert counts to integers
    final_result = final_result.astype({
        'active_endpoint_count': 'int',
        'error_endpoint_count': 'int',
        'count_issue_error_internal': 'int',
        'count_issue_error_external': 'int',
        'count_issue_warning_internal': 'int',
        'count_issue_warning_external': 'int',
        'count_issue_notice_internal': 'int',
        'count_issue_notice_external': 'int'
    })

    provision_table_name = "provision_summary"
    final_result.to_sql(provision_table_name, conn,
                        if_exists='replace', index=False)
    conn.close()


def fetch_reporting_data(db_path):
    conn = sqlite3.connect(db_path)
    query = """
        SELECT 
            rhe.organisation,
            rhe.collection,
            rhe.pipeline,
            rhe.endpoint,
            rhe.endpoint_url,
            rhe.resource,
            rhe.latest_status,
            rhe.exception,
            substring(rhe.endpoint_entry_date,1,10) as endpoint_entry_date,
            substring(rhe.endpoint_end_date,1,10) as endpoint_end_date,
            substring(rhe.resource_start_date,1,10) as resource_start_date,
            substring(rhe.resource_end_date,1,10) as resource_end_date,
            substring(max(rhe.latest_log_entry_date),1,10) as latest_log_entry_date
        FROM 
            reporting_historic_endpoints rhe
        WHERE 
        rhe.endpoint_end_date = ''
        GROUP BY rhe.organisation,rhe.collection, rhe.pipeline,rhe.endpoint
        order by rhe.organisation, rhe.collection
        """
    df_reporting = pd.read_sql_query(query, conn)
    return df_reporting


if __name__ == "__main__":
    level = logging.INFO
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(message)s")

    performance_db_path = sys.argv[1] if len(
        sys.argv) > 1 else "dataset/performance.sqlite3"
    digital_land_db_path = sys.argv[2] if len(
        sys.argv) > 2 else "dataset/digital-land.sqlite3"

    provision_data = fetch_provision_data(digital_land_db_path)
    issue_data = fetch_issue_data(digital_land_db_path)
    cf_data = fetch_column_field_data(digital_land_db_path)
    reporting_data = fetch_reporting_data(performance_db_path)
    reporting_data["organisation"] = reporting_data["organisation"].str.replace(
        "-eng", "")

    provision_reporting_data = pd.merge(provision_data, reporting_data, left_on=[
                                        "organisation", "dataset"], right_on=["organisation", "pipeline"], how="left")
    issue_merged_data = pd.merge(provision_reporting_data, issue_data, left_on=[
                                 "resource", "dataset"], right_on=["resource", "dataset"], how="left")
    cf_merged_data = pd.merge(provision_reporting_data, cf_data, left_on=[
                              "resource", "dataset"], right_on=["resource", "dataset"], how="left")
    # Create new tables and insert data in performance database
    create_performance_tables(
        issue_merged_data, cf_merged_data, performance_db_path)

    logging.info(
        "Tables in 'performance' DB created successfully.")
