#!/usr/bin/env python3

# create a database for exploring collection, specification, configuration and logs

import os
import sys
import csv
import logging
import sqlite3
import duckdb

from pathlib import Path
from urllib.error import HTTPError

from urllib.request import urlretrieve
import pandas as pd
from digital_land.specification import Specification

spec = Specification('specification')
cache_exp_dir = 'var/expectations'
Path(cache_exp_dir).mkdir(parents=True, exist_ok=True)

datasets = spec.dataset
for dataset_obj in datasets.values():
    if dataset_obj['collection']:
        s3="https://files.planning.data.gov.uk"
        dataset = dataset_obj['dataset']
        print(dataset)
        # https://files.planning.data.gov.uk/log/expectations/dataset=ancient-woodland/ancient-woodland.parquet
        try:
            url = f"https://files.planning.data.gov.uk/log/expectation/dataset={dataset}/{dataset}.parquet"
            output_file = cache_exp_dir + f"/{dataset}.parquet"
            urlretrieve(url, output_file)
        except HTTPError as e:
            logging.error(f"unable to download file because {e}")

conn = duckdb.connect()
Path('expectation').mkdir(parents=True, exist_ok=True)
conn.execute(
    f"""
        COPY (
            SELECT * FROM '{cache_exp_dir}/*.parquet'
        ) 
        TO 'expectation/expectation.csv' (FORMAT CSV, HEADER TRUE)
    """
)
