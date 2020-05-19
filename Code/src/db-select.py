#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 15:54:00 2020

@author: selin
"""

import os
import pandas as pd
import psycopg2
from config import config

def get_entries(sql, columns):
    """ create tables in the PostgreSQL database"""
    conn = None
    try:
        params = config('database.ini')
        conn = psycopg2.connect(**params)
        sql_query = pd.read_sql_query(sql, conn)
        df = pd.DataFrame(sql_query, columns=columns)
        conn.close()
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
	# get triplets
	columns = ['object_id','cindex','context']
	sql = "SELECT object_id, cindex, context FROM triplets WHERE context <> 'NOTHING' ORDER BY object_id;"
    triplets = get_entries(sql, columns)
    #triplets = triplets.sample(n=10000)
    path = os.getcwd() + '/df-triplets.parquet.gzip'
    triplets.to_parquet(path, compression='gzip')
	# get types
	columns = ['label','type']
	sql = "SELECT label, type FROM types;"
    types = get_entries(sql, columns)
    path = os.getcwd() + '/df-types.parquet.gzip'
    types.to_parquet(path, compression='gzip')
	
    
