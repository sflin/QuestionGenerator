#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 15:54:00 2020

@author: selin
"""

import psycopg2
from src.config import config

def execute_sql(config_file, commands):
    """Generate a new database with the configuration specified in the config_file.
    
    :param config_file: str, file-path of the configuration-file
    :param commands: str, SQL-commands to execute
    """
    
    conn = None
    try:
        params = config(config_file)
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def create_tables(config_file='database.ini'):
    """Create tables in the PostgreSQL database
    :param config_file: str, optional, file-name, default database.ini
    """
    
    commands = (
        """ CREATE TABLE p_relation (
                relation_id INTEGER UNIQUE,
                label TEXT NOT NULL,
                PRIMARY KEY(relation_id)
        );""",
        
        """CREATE TABLE q_item (
            q_id INTEGER,
            label VARCHAR(255) NOT NULL,
            aliases TEXT,
            url VARCHAR(255) NOT NULL,
            relation_id INTEGER,
            relation_label TEXT,
            value_id INTEGER,
            value VARCHAR(255) NOT NULL,
            value_text TEXT NOT NULL,
            object_type VARCHAR(255),
            value_type VARCHAR(255),
            has_context INTEGER NOT NULL,
            UNIQUE(q_id, relation_id, value),
            PRIMARY KEY (q_id, relation_id, value)
        );
        """,
        """ CREATE TABLE triplets (
                object_id INTEGER NOT NULL,
                object VARCHAR(255),
                relation_id INTEGER NOT NULL,
                relation VARCHAR(255),
                value_id INTEGER NOT NULL,
                value VARCHAR(255),
                object_type VARCHAR(255),
                value_type VARCHAR(255),
                context TEXT,
                pos_tags TEXT,
                dependencies TEXT,
                cindex VARCHAR(255),
                UNIQUE(cindex),
                PRIMARY KEY(cindex)
        );""",
        """CREATE TABLE types (
                object_id INTEGER NOT NULL,
                label VARCHAR(255),
                type_id INTEGER NOT NULL,
                type VARCHAR(255),
                PRIMARY KEY(object_id, type_id)
        );""",
        """INSERT INTO p_relation (relation_id, label)
            VALUES (0, 'NON-Q-ITEM');
        """,
        """INSERT INTO p_relation (relation_id, label)
            VALUES (569, 'Geburtsdatum, am\\b.*geboren, Geburtsjahr, \*');
        """,
        """INSERT INTO p_relation (relation_id, label)
            VALUES (19, 'Geburtsort, in\\b.*geboren, \*');
        """,
        """INSERT INTO p_relation (relation_id, label)
            VALUES (20, 'Sterbeort, Todesort, in\\b.*gestorben, in\\b.*verstorben, †');
        """,
        """INSERT INTO p_relation (relation_id, label)
            VALUES (570, 'Sterbedatum, Todesdatum, am\\b.*gestorben, am\\b.*verstorben, Todestag, Todeszeitpunkt, Todesjahr, Sterbejahr, starb am, †');
        """
        )
    execute_sql(config_file, commands)
         
def drop_tables(config_file='database.ini'):
    """Drop all tables of the database specified in the configuration file.
    :param config_file: str, optional, the filepath of the configuration file
    """
    
    commands = (
        """ DROP TABLE q_item;""",
        """ DROP TABLE p_relation;""",
        """ DROP TABLE triplets;""",
        """ DROP TABLE types;"""
        )
    execute_sql(config_file, commands)
 
if __name__ == '__main__':
    create_tables('database.ini')