#!/usr/bin/env python3

# create a database for exploring collection, specification, configuration and logs

import os
import sys
import csv
import logging
import sqlite3
from digital_land.package.sqlite import SqlitePackage


tables = {
    
}

indexes = {

}


if __name__ == "__main__":
    level = logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    path = sys.argv[1] if len(sys.argv) > 1 else "dataset/performance.sqlite3"
    package = SqlitePackage("performance", path=path, tables=tables, indexes=indexes)
    package.create()
    
    # conn = sqlite3.connect(path)
    # conn.execute("""
    # CREATE TABLE most_recent_log AS
    # select t1.*
    #     from log t1 
    #     inner join (
    #         SELECT endpoint,max(date(entry_date)) as most_recent_log_date
    #         FROM log 
    #         GROUP BY endpoint
    #         ) t2 on t1.endpoint = t2.endpoint
    #     where date(t1.entry_date) = t2.most_recent_log_date
    # """)

    # conn.close()
