#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 15:02:26 2020

@author: selin
"""

import abc
import dateparser
import logging
import psycopg2
import re
import requests
import spacy
import src.utils as utils
import timeit
import unidecode
from bs4 import BeautifulSoup
from itertools import chain
from multiprocessing import Pool
from src.config import config

class Filler(metaclass=abc.ABCMeta):
    
    def fill(self, config_file='database.ini'):
        """Entry point of class and template method. Fill triplets either for 
            given seeds or for any non-filled items in dataset.
    
        :param config_file: str, optional filename of database-configuration to use
        """
        try:
            start_time = timeit.default_timer()
            seconds = 0
            rows_count = 0
            params = config(config_file)
            connection = psycopg2.connect(**params)
            cursor = connection.cursor()
            rows = self.get_rows(cursor)
            row_chunks = utils.get_chunks(rows)
            for chunks in row_chunks:
                rows_count += 1
                with Pool() as pool:
                    res = list(pool.imap(self.get_data, chunks))
                    insert_list = list(chain(*res))
                    if insert_list:
                        insert_sql = self.insert_list()
                        cursor.executemany(insert_sql, insert_list)
                        connection.commit()
            connection.close()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Filler: " + str(error))
            logging.critical("Filler: " + str(error))
        finally:
            second_time = timeit.default_timer() - start_time
            seconds += second_time
            log = "Processed " + str(rows_count) + " chunks à max 50 of items in " + str(second_time/60) + "" + " minutes"
            logging.info(log)

    @abc.abstractmethod
    def get_rows(self, cursor):
        pass

    @abc.abstractmethod
    def get_data(self, row):
        pass

    @abc.abstractmethod
    def insert_list(self):
        pass

class ObjectFiller(Filler):
    
    def __init__(self, seeds=None):
        """:param seeds: array_like, optional, list of starting-seeds"""
        
        self.seeds = seeds
        self.formats = ['%d. %B %y', '%d. %b %Y', '%d. %b %y', '%d. %B %Y', 
                        '%d.%m.%y', '%d.%m.%Y', '%d-%m-%y', '%d-%m-%Y', 
                        '%d-%b-%Y']
    
    def get_rows(self, cursor):
        """ Return Q-IDs to fill; seeds if provided, relation-values otherwise.
        :param cursor: a cursor object
        :return rows: array_like, list of retrieved rows"""
        
        if self.seeds:
            return self.seeds
        sql = """SELECT DISTINCT q1.value_id FROM q_item q1 WHERE 
            q1.value_id <> 0 AND NOT EXISTS (SELECT 1 FROM q_item q2 
            WHERE q2.q_id = q1.value_id);""" 
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    
    def get_q_item_data(self, data, item_id):
        """Parses data-dictionary and extracts triplets. Triplets are stored in 
        database.
        
        :param data: dict, dictionary of a wikidata-entity
        :param item_id: str, ID of entity to extract (e.g. Q31)
        :return: array_like, list containing the q_id, label, synonyms, url, 
                             relation_id, relation_value, and is_q_item"""
        try:
            if 'de' in data['labels']:
                label = data['labels']['de']['value']
            elif 'en' in data['labels']:
                label = data['labels']['en']['value']
            else:
                logging.info("Item Q" + str(item_id) + " has no label")
                return []
            if label.startswith('Kategorie') or label.startswith('Category'):
                logging.info("Item Q" + str(item_id) + " is a Category-label " + label)
                return []
            label = unidecode.unidecode(label)
            if 'dewiki' in data['sitelinks'] and 'url' in data['sitelinks']['dewiki']:
                url = data['sitelinks']['dewiki']['url']
                has_context = 0
            else:
                url = 'INVALID URL'
                has_context = 2
            if 'de' in data['aliases']:
                aliases = sorted(list(set([unidecode.unidecode(item['value']) 
                                           for item in data['aliases']['de']])), key=len, reverse=True)
                aliases = [a for a in aliases if a != "" and a != '[?]']
                aliases = ', '.join(item for item in aliases) 
                aliases = label + ', ' + aliases
            elif 'en' in data['aliases']:
                aliases = sorted(list(set([unidecode.unidecode(item['value']) 
                                           for item in data['aliases']['en']])), key=len, reverse=True)
                aliases = [a for a in aliases if a != "" and a != '[?]']
                aliases = ', '.join(item for item in aliases) 
                aliases = label + ', ' + aliases
            else:
                aliases = label
            aliases = re.sub(r"\(", "\\(", aliases)
            aliases = re.sub(r"\)", "\\)", aliases)
            aliases = re.sub(r"\.", "\\.", aliases)
            aliases = re.sub(r"\+", "\\+", aliases)
            aliases = re.sub(r"\*", "\\*", aliases)
            claims = data['claims']
            result = []
            for rel in claims:
                for i in range(len(claims[rel])):
                    value_id = 0
                    rel_label = '?'
                    value = '?'
                    value_text = '?'
                    datatype = claims[rel][i]['mainsnak']['datatype']
                    value_type = '?'
                    if datatype == 'wikibase-item' and 'datavalue' in claims[rel][i]['mainsnak']:
                        value_id = claims[rel][i]['mainsnak']['datavalue']['value']['numeric-id']
                        value = str(value_id)
                    elif datatype == 'string' and 'datavalue' in claims[rel][i]['mainsnak']:
                        value = claims[rel][i]['mainsnak']['datavalue']['value']
                        value = unidecode.unidecode(value)
                        value = re.sub(r"\(", "\\(", value)
                        value = re.sub(r"\)", "\\)", value)
                        value = re.sub(r"\.", "\\.", value)
                        value = re.sub(r"\+", "\\+", value)
                        value = re.sub(r"\*", "\\*", value)
                        value_text = value
                        value_type = 'string'
                        if not value:
                            continue
                    elif datatype == 'quantity' and 'datavalue' in claims[rel][i]['mainsnak']:
                        # TODO: resolve and add unit?
                        amount = claims[rel][i]['mainsnak']['datavalue']['value']['amount'] 
                        # unit = claims[rel][0]['mainsnak']['datavalue']['value']['amount'] 
                        value = re.sub(r"\+", "", amount) # + unit
                        value_text = value
                        value_type = 'quantity'
                    elif datatype == 'time' and 'datavalue' in claims[rel][i]['mainsnak']:
                        time = claims[rel][i]['mainsnak']['datavalue']['value']['time']
                        date = dateparser.parse(time)
                        if date:
                            value = ', '.join([date.strftime(f) for f in self.formats])
                            value_text = value
                            value_type = 'time'
                        else:   
                            # ignore invalid dates
                            continue 
                    else:     
                        # ignore datatypes such as math, globe-coordinate, 
                        # external-id, monolingual text and Commons-Media (e.g. Pictures)
                        continue
                    #columns = ['QId','Label','Aliases','URL', 
                    #           'RelationId','RelationLabel','ValueId','Value',
                    #           'ValueText','hasContext']
                    result.append((int(item_id), label[:250], aliases, url, 
                                   int(rel[1:]), rel_label, int(value_id), value[:250], 
                                   value_text, value_type, has_context))
            return result
        except (Exception) as error:
            print("ObjectFiller: " + str(error))
            logging.critical("ObjectFiller: " + str(error))

    
    def get_data(self, row):
        """Get data for Q-item and extract triplets of it
        
        :param row: array_like, list containing Q-item-ID to retrieve the label for
        :return: array_like, list containing the q_id, label, synonyms, url, 
                             relation_id, relation_value, and is_q_item"""
        
        row_id = row[0]
        data = utils.get_data('Q' + str(row_id))
        q_data = []
        if data:
            q_data = self.get_q_item_data(data, row_id)
        return q_data

    def insert_list(self):
        """Generate object-specific SQL for inserting values in insert-list 
        
        :return insert_sql: str, the insert-SQL-command"""
        
        #columns = ['QId','Label','Aliases','URL', 
                    #           'RelationId','RelationLabel','ValueId','Value',
                    #           'ValueText','hasContext']
        insert_sql = """INSERT INTO q_item (q_id,label,aliases,url,relation_id,
                        relation_label,value_id,value,value_text, value_type ,has_context) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                        ON CONFLICT DO NOTHING;"""
        return insert_sql


class RelationFiller(Filler):
    
    def get_rows(self, cursor): 
        """ Return Relation-IDs to fill.
        :param cursor: a cursor object
        :return rows: array_like, list of relation IDs to fill"""
        
        sql = """SELECT DISTINCT q.relation_id FROM q_item q WHERE NOT EXISTS 
            (SELECT 1 FROM p_relation p WHERE p.relation_id =
                     CAST(q.relation_id AS INTEGER));"""
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    def get_data(self, row):
        """Insert a new relation into the relations-table
    
        :param data: dict, dictionary containing the data for the relation
        :param item_id: str, ID of the relation, e.g. P31
        :return row_id: int, the id of the P-Relation
        :return label: str, the label of the relation with synonyms, 
                            INVALID if not existent
        """
        row_id = row[0]
        data = utils.get_data('P' + str(row_id))
        label = 'INVALID'
        if data:
            try:
                if 'de' in data['labels']:
                    label = data['labels']['de']['value']
                elif 'en' in data['labels']:
                    label = data['labels']['en']['value']
                else:
                    raise Exception("Relation " + str(row_id) + " has no valid label")
                if label.startswith('Commons') or label.startswith('Hauptkategorie') or label.startswith('Arbeitsliste'):
                    label = 'INVALID'
                    return [(row_id, label)]
                if 'de' in data['aliases']:
                    for item in data['aliases']['de']:
                        rel = item['value'].split(' ')
                        if len(rel) == 2 and (str(rel[0]).startswith('ge') or str(rel[0]).startswith('ver') or
                                              str(rel[0]).startswith('be') or str(rel[0]).startswith('ent')
                                              ) and (str(rel[0]).endswith('d') or str(rel[0]).endswith('t') or 
                                                     str(rel[0]).endswith('n')):
                            rel = "%s\\b.*%s" % (str(rel[1]), str(rel[0]))
                        else:
                            rel = item['value']
                        label = label + ', ' + rel
                label = label.replace('/', ', ')
                label = unidecode.unidecode(label)
                label = re.sub(r"\(.+?\)", "", label)
                label = re.sub(r" \.\.\.", "", label)
                label = re.sub(r"\s?\[.+?\]\s?,?", "", label)
                label = re.sub(r"\+", "\\+", label)
                label = re.sub(r"\*", "\\*", label)
                label = re.sub(r"\.", "\\.", label)
                label = re.sub(r"\\\.\\\*", ".*", label)
                if not label:
                    label = 'INVALID'
            except (Exception) as error:
                print(error)
                logging.critical('RelationFiller: ' + str(error))
        return [(row_id, label)]

    def insert_list(self):
        """Generate object-specific SQL for inserting values in insert-list """
        insert_sql = """INSERT INTO p_relation (relation_id, label) 
                VALUES (%s,%s) ON CONFLICT DO NOTHING;"""
        return insert_sql

class TripletFiller(Filler):
    
    def get_rows(self, cursor):
        """ Return rows with attributes to fill context.
        :param cursor: a cursor object
        :return rows: array_like, list attributes"""
        
        sql = """SELECT DISTINCT q_id, aliases, url, 
        ARRAY_AGG 
        (DISTINCT relation_label || '||' || value_text || '||' || 
        relation_id || '||' || value_id) lab 
        FROM q_item 
        WHERE has_context = 0 AND relation_label <> '?' AND value_text <> '?' 
        AND url <> '' GROUP BY q_id, aliases, url;"""
        
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    
    def get_data(self, row):
        """Retrieve keywords for the given row from wikipedia article.
        
        :param row: array_like, a row containing ID, synonyms of Q-Object, URL 
                                for article and array describing relation to extract
        :return result: array_like, list containing retrieved context, PoS-tags, 
                                    dependency structure, Q-Object ID, 
                                    P-Relation ID, ID of relation value
        """
        
        try:
            ################ LOAD AND PRE-PROCESS TEXT ########################
            url = row[2] + "?action=render"
            response = requests.get(url)
            html = BeautifulSoup(response.text, 'html.parser')
            paragraphs = html.select("p")
            content = "".join([para.text for para in paragraphs])
            content = unidecode.unidecode(content)
            content = content.replace(";", ",")
            content = content.replace(" |", ",")
            content = re.sub(r"(\s+|\n)", " ", content)
            content = re.sub(r"[\'`\"']|(\[[^]]*\])|(,,)|(\( .*\?\/i\)\s)", "",content)
            nlp = spacy.load("de_core_news_sm")
            doc = nlp(content)
            sentences = list(doc.sents)
            ####################### FIND TITLE IN TEXT ########################
            alias_matches = []
            result = []
            context = ''
            alias_seen = False
            obj_aliases = row[1].split(', ')
            for i in range(1, len(sentences)-1):
                for oa in obj_aliases:
                    search_text = sentences[i-1].text + ' ' + sentences[i].text + ' ' + sentences[i+1].text
                    span_tuple = (sentences[i-1], sentences[i], sentences[i+1], str(oa))
                    match = re.findall(r'\b%s\b' % oa, search_text)
                    if match and span_tuple not in alias_matches:
                        alias_matches.append(span_tuple)
                        alias_seen = True
            agg_items = row[3]
            for ai in agg_items:
                """ai is EITHER
                'Geburtsdatum, am\b.*geboren, \*||11. März 52||569||11. März 52'
                OR
                'Geburtsort, in\b.*geboren, \*||Cambridge, city of Cambridge||19||350'
                """
                pos_tags = ''
                deps = ''
                rel_matches = []
                relations = []
                rel_seen = False
                items = ai.split('||')
                rel_val = items[0]
                relations = rel_val.split(', ')
                val_aliases = items[1].split(', ')
                rel_id = items[2]
                rel_value_id = int(items[3])
                ######################## PRE-PROCESS PATTERNS #####################
                if alias_seen:
                    for rel in relations:
                        if rel == 'INVALID':
                            continue
                        elif rel == '\*' or rel == '†':
                            pattern = r'%s' % rel
                        else:
                            pattern = re.compile(r'\b%s\b' % rel)
                        for am in alias_matches:
                            search_text = am[0].text + ' ' + am[1].text + ' ' + am[2].text
                            span_tuple = (am[0], am[1], am[2], am[3], str(rel))
                            match = re.findall(pattern, search_text)
                            if match and span_tuple not in rel_matches:
                                rel_matches.append(span_tuple)
                                rel_seen = True
                    if rel_seen:
                        for va in val_aliases:
                            for rm in rel_matches:
                                context = rm[0].text + ' ||| ' + rm[1].text + ' ||| ' + rm[2].text
                                match = re.findall(r'\b%s\b' % va,  context)
                                if match:
                                    cindex = rm[3][:80] + '||' +  rm[4][:60] + '||' +  va[:80] + '||' +  context[:20] # cindex has a max length of 246 chars --> 255 allowed!
                                    pos_tags = pos_tags + ', '.join([item.tag_ for item in rm[0]]) +  ' ||| ' + ', '.join([item.tag_ for item in rm[1]]) +  ' ||| ' + ', '.join([item.tag_ for item in rm[2]])
                                    deps = deps + ', '.join([item.dep_ for item in rm[0]]) +  ' ||| ' + ', '.join([item.dep_ for item in rm[1]]) +  ' ||| ' + ', '.join([item.dep_ for item in rm[2]])
                                    # (object_id, object, relation_id, relation, value_id, value, context, pos_tags, dependencies, cindex)
                                    result.append((int(row[0]), rm[3], int(rel_id), rm[4], rel_value_id, va, context, pos_tags, deps, cindex))
                    else:
                        # (object_id, object, relation_id, relation, value_id, value, context, pos_tags, dependencies, cindex)
                        cindex = str(row[0]) + '||' + str(rel_id) + '||' +  str(rel_value_id) + '||' +  'NOTHING'
                        result.append((int(row[0]), None, int(rel_id), None, rel_value_id, None, 'NOTHING', None, None, cindex))
            return result
        except(Exception) as e:
            error = str(e) + " While requesting " + row[2]
            print(error)
            logging.critical("TripletFiller: " + str(error))
            return []
    
    def insert_list(self):
        """Generate object-specific SQL for inserting values in insert-list 
        :return insert_sql: str, SQL-statement
        """
        
        insert_sql = """INSERT INTO triplets (object_id, object, relation_id,
                    relation, value_id, value, context, pos_tags, dependencies, cindex) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;"""
        return insert_sql

class ObjectTypeFiller(Filler):
    
    def get_rows(self, cursor):
        """ Return QId, relation-value to fill in as object-types.
        
        :param cursor: a cursor object
        :return: array_like, list of retrieved rows"""
        
        sql = """SELECT DISTINCT q_id, 
        ARRAY_AGG(DISTINCT value) agg FROM q_item 
        WHERE object_type IS NULL AND relation_id=31
        GROUP BY q_id ORDER BY q_id;"""
        cursor.execute(sql)
        tmp1 = cursor.fetchall()
        sql = """SELECT DISTINCT q_id, 
        ARRAY_AGG(DISTINCT value) agg FROM q_item 
        WHERE object_type IS NULL AND relation_id=279
        GROUP BY q_id ORDER BY q_id;"""
        sql = """SELECT 
        DISTINCT q_id, value, value_id, label FROM q_item 
        WHERE relation_id=31;"""
        cursor.execute(sql)
        tmp1 = cursor.fetchall()
        sql = """SELECT 
        DISTINCT q_id, value, value_id, label FROM q_item 
        WHERE relation_id=279;"""
        cursor.execute(sql)
        tmp2 = cursor.fetchall()
        firsts = [t[0] for t in tmp1]
        tmp2 = [t for t in tmp2 if t[0] not in firsts]
        return tmp1 + tmp2

    def get_data(self, row):
        """Find label in data
        
        :param row: array_like, list containing item_id, relation_value_id to retrieve the labels
        :return: array_like, list containing the label, and the item_id"""
                
        item_id = row[0]
        label = 'UNKNOWN'
        if row[1].isdigit():
            data = utils.get_data('Q' + row[1])
            if data:
                if 'de' in data['labels']:
                    label = data['labels']['de']['value']
                elif 'en' in data['labels']:
                    label = data['labels']['en']['value']
            label = unidecode.unidecode(label)
        else:
            label = row[1]
        if label != 'UNKNOWN':
            return [(item_id, label, row[2], row[3])]
        else:
            return []

    def insert_list(self):
        """Generate object-specific SQL for inserting values in insert-list 
        :return insert_sql: str"""
        
        insert_sql = """INSERT INTO types (object_id, type, type_id, label) 
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING;"""
        #insert_sql = """UPDATE q_item SET object_type = %s 
        #            WHERE q_id = %s AND object_type IS NULL;"""
        return insert_sql
