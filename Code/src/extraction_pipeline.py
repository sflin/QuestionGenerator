#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 13 15:57:30 2020

@author: selin
"""
import gc
import locale
import logging
import os
import psycopg2
import time
import timeit
from config import config
from Filler import (ObjectFiller, RelationFiller, TripletFiller, ObjectTypeFiller)

def join_table_values():
    """"Update q_items; insert all values from other tables to avoid joins."""
    try:
        params = config()
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        sql = """SELECT label, relation_id FROM p_relation;"""
        cursor.execute(sql)
        insert_list = cursor.fetchall()
        insert_sql = """UPDATE q_item set relation_label=%s 
            WHERE relation_id=%s AND relation_label = '?';"""
        cursor.executemany(insert_sql, insert_list)
        connection.commit()
        sql = """SELECT distinct label, aliases, q_id, label from q_item;"""
        cursor.execute(sql)
        insert_list = cursor.fetchall()
        insert_sql = """UPDATE q_item set value = %s, value_text = %s
            WHERE value_id = %s AND value_text = '?' 
            AND NOT EXISTS (SELECT 1 FROM q_item WHERE value = %s);"""
        cursor.executemany(insert_sql, insert_list)
        connection.commit()
        """# ignore all values which start with a '+'
        sql = "UPDATE q_item SET has_context=2 WHERE value LIKE '+%';"
        cursor.execute(sql)
        connection.commit()"""
        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 

def clean_up_triplets():
    """Update q_items in order to avoid looking up triplets multiple times."""
    
    try:
        params = config()
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        sql = """UPDATE q_item q set has_context = 2 WHERE EXISTS
                (SELECT 1 FROM triplets tr 
                WHERE q.q_id = tr.object_id AND 
                q.relation_id = tr.relation_id AND 
                q.value_id = tr.value_id AND
                tr.context = 'NOTHING');"""
        cursor.execute(sql)
        sql = """UPDATE q_item q set has_context = 1 WHERE EXISTS
                (SELECT 1 FROM triplets tr 
                WHERE q.q_id = tr.object_id AND 
                q.relation_id = tr.relation_id AND 
                q.value_id = tr.value_id AND
                tr.context <> 'NOTHING');"""
        cursor.execute(sql)
        sql = """DELETE FROM triplets WHERE context='NOTHING';"""
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 

def calculate_statistics(start_time, seconds):
    """Calculate statistics in database and create log entries.
    :param start_time: float, point in time when execution has started
    :param seconds: float, elapsed time in seconds since start"""
    
    try:
        params = config()
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        sql = "SELECT count(*) FROM q_item;"
        cursor.execute(sql)
        q_count = cursor.fetchall()
        sql2 = "SELECT count(*) FROM p_relation;"
        cursor.execute(sql2)
        p_count = cursor.fetchall()  
        sql3 = "SELECT count(*) FROM triplets WHERE context <> 'NOTHING';"
        cursor.execute(sql3)
        context_count = cursor.fetchall()  
        second_time = timeit.default_timer() - start_time
        seconds += second_time
        minutes = seconds / 60
        log = "Database contains now " + str(q_count[0][0]) + " q-objects, " + str(p_count[0][0]) + " p-relations, and " + str(context_count[0][0]) + " context-objects. Processed in " + str(minutes) + " minutes."
        logging.info(log)
        connection.close()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)  
        
def run(seeds = []):
    """Entry point of extraction pipeline.
    :param seeds: array_like, an optional set of Q-IDs provided as starting-seeds"""
    try:
        logging.info("Starting execution")
        logging.info("Getting objects")
        locale.setlocale(locale.LC_TIME, 'de_DE.utf8')
        filler = ObjectFiller(seeds = seeds)
        filler.fill()
        logging.info("Getting relations")
        re_filler = RelationFiller()
        re_filler.fill()
        join_table_values()
        logging.info("Getting triplets")        
        triplet_filler = TripletFiller()
        triplet_filler.fill()
        clean_up_triplets()
    except (Exception) as e:
        print(e)
        logging.critical('extraction_pipeline.py: ' + str(e))
        
if __name__ == '__main__':
    start_time = timeit.default_timer()
    seconds = 0
    cwd = os.getcwd()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S", filemode='w', level=logging.INFO,
                        filename = cwd + '/logs/db-filler-log' + str(time.time()).replace('.','') + '.log')
    # 42: Douglas Adams, 34660: Joanne K. Rowling, 5879: J.Goethe, 1339: J.S.Bach, 
    # 1299: The Beatles, 47875: Robbie Williams, 584: Rhine, 513: Mount Everest, 
    # 207773: Howard Shore, 1374: Matterhorn
    seeds = [[42],[34660],[5879],[1339],[1299],[47875],[584],[513],[207773],[1374]]
    run(seeds)
    for i in range(10):
        run(seeds=None)
        gc.collect()
    logging.info("Fill object types")
    obj_val_filler = ObjectTypeFiller()
    obj_val_filler.fill()
    gc.collect()
    logging.info("Execution finished.")
    calculate_statistics(start_time, seconds)