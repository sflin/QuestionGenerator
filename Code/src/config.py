#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 16:06:28 2020

@author: selin
"""

from configparser import ConfigParser

def config(filename='database.ini', section='postgresql'):
    """Reads and creates a database-configuration from a file.
    :param filename: str, filepath of configuration file
    :param section: str, part in file to process
    :return db: Database-object"""
    
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

if __name__ == '__main__':
    db = config('database.ini', section='postgresql')