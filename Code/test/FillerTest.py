#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 09:58:35 2020

@author: selin
"""
import locale
import psycopg2
import unittest
from src.database import create_tables, drop_tables
from src.Filler import (ObjectFiller, RelationFiller, TripletFiller, ObjectTypeFiller)
from testconfig import config
        
def join_table_values():
    try:
        params = config('testdatabase.ini')
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        sql = """SELECT label, relation_id FROM p_relation;"""
        cursor.execute(sql)
        insert_list = cursor.fetchall()
        insert_sql = """UPDATE q_item set relation_label=%s 
            WHERE relation_id=%s AND relation_label = '?';"""
        cursor.executemany(insert_sql, insert_list)
        connection.commit()
        sql = """SELECT distinct label, aliases, q_id from q_item;"""
        cursor.execute(sql)
        insert_list = cursor.fetchall()
        insert_sql = """UPDATE q_item set value = %s, value_text = %s
            WHERE value_id = %s AND value_text = '?';"""
        cursor.executemany(insert_sql, insert_list)
        connection.commit()
        # ignore all values which start with a '+'
        sql = "UPDATE q_item SET has_context=2 WHERE value LIKE '+%';"
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 

def clean_up_triplets():
    """Update q_items in order to avoid looking up triplets multiple times."""
    
    try:
        params = config('testdatabase.ini')
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
        
def fill_value_types():
    """ Update q_item and set the value type."""
    
    try:
        params = config('testdatabase.ini')
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        sql = """SELECT DISTINCT object_type, q_id
                    FROM q_item WHERE object_type <> '';"""
        cursor.execute(sql)
        insert_list= cursor.fetchall()
        insert_sql = """UPDATE q_item SET value_type = %s 
                        WHERE value_id = %s AND value_type IS NULL;"""
        cursor.executemany(insert_sql, insert_list)
        connection.commit()
        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)    

def add_types_to_triplets():
    """Adds object and value types to the triplets table."""
    
    try:
        params = config('testdatabase.ini')
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        sql = """SELECT DISTINCT object_type, q_id 
                    FROM q_item WHERE object_type <> '';"""
        cursor.execute(sql)
        insert_list= cursor.fetchall()
        insert_sql = """UPDATE triplets SET object_type = %s 
                        WHERE object_id = %s and object_type IS NULL;"""
        cursor.executemany(insert_sql, insert_list)
        connection.commit()
        sql = """SELECT DISTINCT value_type, q_id, value_id 
                    FROM q_item WHERE object_type <> '';"""
        cursor.execute(sql)
        insert_list= cursor.fetchall()
        insert_sql = """UPDATE triplets SET value_type = %s 
                        WHERE object_id = %s and value_id = %s 
                        and value_type IS NULL;"""
        cursor.executemany(insert_sql, insert_list)
        connection.commit()
        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)  
        
def get_count():
    try:
        params = config('testdatabase.ini')
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        sql = "SELECT count(*) FROM q_item;"
        cursor.execute(sql)
        q_count = cursor.fetchall()[0][0] 
        sql = "SELECT count(*) FROM types;"
        cursor.execute(sql)
        ot = cursor.fetchall()[0][0] 
        sql2 = "SELECT count(*) FROM p_relation;"
        cursor.execute(sql2)
        p_count = cursor.fetchall()[0][0]  
        sql3 = "SELECT count(*) FROM triplets;"
        cursor.execute(sql3)
        context_with = cursor.fetchall()[0][0]
        sql4 = "SELECT count(*) FROM triplets WHERE context <> 'NOTHING';"
        cursor.execute(sql4)
        context_wo = cursor.fetchall()[0][0]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return q_count, p_count, context_with, context_wo, ot

class ObjectFillerTest (unittest.TestCase):
    
    def setUp(self):
        create_tables('testdatabase.ini')
    
    def tearDown(self):
        drop_tables('testdatabase.ini')
    
    def test_with_seeds(self):
        # SETUP
        seeds = [[42]]
        tester = ObjectFiller(seeds=seeds)
        # SUT
        tester.fill('testdatabase.ini')
        # VERIFY
        q_count, p_count, context_count, context_wo, ot = get_count()
        self.assertEqual(q_count, 56)
        self.assertEqual(p_count, 5)
        self.assertEqual(context_count, 0)
        self.assertEqual(context_wo, 0)
        self.assertEqual(ot, 0)
    
    def test_second_round(self):
        # SETUP
        tester = ObjectFiller(seeds=[[42]])
        tester.fill('testdatabase.ini')
        tester = ObjectFiller()
        # SUT
        # run it a second time
        tester.fill('testdatabase.ini')
        # VERIFY
        q_count, p_count, context_count, context_wo, ot = get_count()
        self.assertEqual(q_count, 1905) 
        self.assertEqual(p_count, 5)
        self.assertEqual(context_count, 0)
        self.assertEqual(context_wo, 0)
        self.assertEqual(ot, 0)
        
        
class RelationFillerTest (unittest.TestCase):
    
    def setUp(self):
        create_tables('testdatabase.ini')
    
    def tearDown(self):
        drop_tables('testdatabase.ini')
        
    def test(self):
        # SETUP
        tester = ObjectFiller(seeds=[[42]])
        tester.fill('testdatabase.ini')
        tester = RelationFiller()
        # SUT
        tester.fill('testdatabase.ini')
        # VERIFY
        q_count, p_count, context_count, context_wo, ot = get_count()
        self.assertEqual(q_count, 56)
        self.assertEqual(p_count, 38)
        self.assertEqual(context_count, 0)
        self.assertEqual(context_wo, 0)
        self.assertEqual(ot, 0)
        
class TripletFillerTest (unittest.TestCase):
    
    def test(self):
        # SETUP
        tester = ObjectFiller(seeds=[[42]])
        tester.fill('testdatabase.ini')
        tester = RelationFiller()
        tester.fill('testdatabase.ini')
        join_table_values()
        tester = TripletFiller()
        # SUT
        tester.fill('testdatabase.ini')
        #clean_up_triplets()
        # VERIFY
        q_count, p_count, context_count, context_wo, ot = get_count()
        self.assertEqual(q_count, 56)
        self.assertEqual(p_count, 38)
        self.assertEqual(context_count, 6) 
        self.assertEqual(context_wo, 4)
        self.assertEqual(ot, 0)

    def setUp(self):
        create_tables('testdatabase.ini')
    
    def tearDown(self):
        drop_tables('testdatabase.ini')
        
class ObjectTypeFillerTest (unittest.TestCase):
    def setUp(self):
        create_tables('testdatabase.ini')
    
    def tearDown(self):
        drop_tables('testdatabase.ini')
    
    def test(self):
        # SETUP
        tester = ObjectFiller(seeds=[[42]])
        tester.fill('testdatabase.ini')
        tester = ObjectFiller()
        tester.fill('testdatabase.ini')
        tester = RelationFiller()
        tester.fill('testdatabase.ini')
        join_table_values()
        tester = ObjectTypeFiller()
        # SUT
        tester.fill('testdatabase.ini')
        # VERIFY
        q, p, cc, cwo, ot = get_count()
        
        self.assertEqual(ot, 81)

class TestDataGenerator (unittest.TestCase):  
    
    def setUp(self):
        create_tables('testdatabase.ini')
    
    def test(self):
        # SETUP
        tester = ObjectFiller(seeds=[[42]]) # 54
        tester.fill('testdatabase.ini')
        tester = ObjectFiller() # 1711
        tester.fill('testdatabase.ini')
        tester = RelationFiller() # 538
        tester.fill('testdatabase.ini')
        join_table_values()
        tester = TripletFiller()
        tester.fill('testdatabase.ini')
        clean_up_triplets()
        tester = ObjectTypeFiller()
        tester.fill('testdatabase.ini')
        
if __name__ == '__main__':
    # Hint: if the test fail, it might be possible that more items were added in wikidata and thus the numbers are incorrect!
    locale.setlocale(locale.LC_TIME, 'de_DE.utf8') 
    suite = unittest.TestLoader().loadTestsFromTestCase(ObjectFillerTest)
    unittest.TextTestRunner(verbosity=5).run(suite)
    suite = unittest.TestLoader().loadTestsFromTestCase(RelationFillerTest)
    unittest.TextTestRunner(verbosity=5).run(suite)
    suite = unittest.TestLoader().loadTestsFromTestCase(TripletFillerTest)
    unittest.TextTestRunner(verbosity=5).run(suite)
    suite = unittest.TestLoader().loadTestsFromTestCase(ObjectTypeFillerTest)
    unittest.TextTestRunner(verbosity=5).run(suite)
    # to generate some test-data:
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDataGenerator)
    #unittest.TextTestRunner(verbosity=5).run(suite)