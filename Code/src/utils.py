#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 14:41:57 2020

@author: selin
"""
import json
import requests
import logging

def get_data(item_id):
    """Helper method to retrieve data from wikidata.
    
    :param item_id: int, ID of entity to retrieve (e.g. P123 or Q123)
    :returns data[item_id]: dict, the wikidata-item as a dictionary"""
    
    try:
        if item_id:
            link = "https://www.wikidata.org/wiki/Special:EntityData/" + item_id + ".json"
            page = requests.get(link)
            if 'entities' in page.text:
                data = json.loads(page.text)['entities']
            else:
                return
        if item_id in data: # ATTENTION: we get nothing back if item is re-directed from another one!
            return data[item_id]
    except (Exception) as error:
        logging.critical(error)
        
def get_chunks(lst, n=500):
    """Helper method that yields successive n-sized chunks from a list lst.
    
    :param lst: array_like, a list
    :param n: int, chunk-size optional
    :yield: array_like, n-sized chunks from lst"""
    
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
def merge_rows(l1, l2):
    """Helper method to merge two lists of rows with inequal length into a 
    single long one based on the q-object-ID.
    
    :param l1: array_like, the first list of rows
    :param l2: array_like, the second list of rows
    :return: array_like, a single long list """
    
    long_list = l2 if len(l2) > len(l1) else l1
    short_list = l1 if len(l2) > len(l1) else l2
    for x in short_list:
        is_contained = False
        for y in long_list:
            if x[0] == y[0]:
                is_contained = True
                for arr in x[3]:
                    y[3].append(arr)
                break
        if not is_contained:
            long_list.append(x)
    return long_list