# -*- coding: utf-8 -*-
"""
Created on:     Sun May 21 05:09:40 2017
Author:         Waldu Woensdregt
Description:    Functions used by msc_CollectData.py
"""
import sqlite3
from datetime import datetime


def open_db_conn(db_file=r'/home/openwpm/Desktop/crawl-data.sqlite'):
    """"
    open connection to sqlite database
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)
    return None


def create_table(conn, table_name, table_sql):
    """
    creates tables required for the masters project
    """
    sql_str = " SELECT count(1) \
                FROM sqlite_master \
                WHERE type='table' \
                    AND name='{0}' \
              ".format(table_name)
    if conn.execute(sql_str).fetchone()[0] == 0:
        # table does not exist -- create
        conn.execute(table_sql)
        print 'Table [{}] created'.format(table_name)
    else:
        print 'Table [{}] already exists'.format(table_name)


def setup_db_tables(conn, myprefix, recreate=0):
    """
    Create tables for the MSc if they do not exist
    run with "1" as third parameter to recreate tables
    NOTE: using "1" will delete all data populated into these tables
    """

    if recreate == 1:
        # drop all tables since they will be recreated
        conn.execute('DROP TABLE {}'.format(myprefix + 'param_values'))
        conn.execute('DROP TABLE {}'.format(myprefix + 'ad_platforms'))
        conn.execute('DROP TABLE {}'.format(myprefix + 'ap_domains'))
        conn.execute('DROP TABLE {}'.format(myprefix + 'sensitivity'))
        conn.execute('DROP TABLE {}'.format(myprefix + 'param_classification'))
        conn.execute('DROP TABLE {}'.format(myprefix + 'known_values'))
        conn.execute('DROP TABLE {}'.format(myprefix + 'known_keys'))
        print 'All "{}" tables dropped'.format(myprefix)

    # table for individual parameters and their values
    # - used by "AddParamIntoDb"
    table_name = myprefix + 'param_values'
    sql_str = ' \
        CREATE TABLE {} ( \
             par_val_id INTEGER PRIMARY KEY AUTOINCREMENT \
             ,http_response_id INT \
             ,sub_domain VARCHAR(150) \
             ,param_key VARCHAR(150) \
             ,param_value VARCHAR(1000) \
             ,created_dt TEXT \
             ,ap_id INT); \
        '.format(table_name)
    create_table(conn, table_name, sql_str)

    # ad_platforms table
    table_name = myprefix + 'ad_platforms'
    sql_str = ' \
        CREATE TABLE {} ( \
        ap_id INTEGER PRIMARY KEY AUTOINCREMENT, \
        ap_name TEXT NOT NULL, \
        ap_main_url TEXT, \
        ghostery_link TEXT); \
        '.format(table_name)
    create_table(conn, table_name, sql_str)

    # ap_domains table
    table_name = myprefix + 'ap_domains'
    sql_str = ' \
        CREATE TABLE {} ( \
        pdom_id INTEGER PRIMARY KEY AUTOINCREMENT, \
        ap_id INT NOT NULL, \
        pdom_domain TEXT NOT NULL, \
        FOREIGN KEY(ap_id) REFERENCES {}(ap_id)); \
        '.format(table_name, myprefix + 'ad_platforms')
    create_table(conn, table_name, sql_str)

    # sensitivity category table
    table_name = myprefix + 'sensitivity'
    sql_str = ' \
        CREATE TABLE {} ( \
        sensitivity_id INTEGER PRIMARY KEY AUTOINCREMENT, \
        category TEXT NOT NULL, \
        description TEXT, \
        score INT); \
        '.format(table_name)
    create_table(conn, table_name, sql_str)

    # param_classification table
    table_name = myprefix + 'param_classification'
    sql_str = ' \
        CREATE TABLE {} ( \
        par_class_id INTEGER PRIMARY KEY AUTOINCREMENT, \
        par_val_id INT NOT NULL, \
        is_personal BOOLEAN, \
        sensitivity_id INT, \
        FOREIGN KEY(sensitivity_id) REFERENCES {}(sensitivity_id)); \
        '.format(table_name, myprefix + 'sensitivity')
    create_table(conn, table_name, sql_str)

    # known_values table
    table_name = myprefix + 'known_values'
    sql_str = ' \
        CREATE TABLE {} ( \
        id INTEGER PRIMARY KEY AUTOINCREMENT, \
        knownvalue TEXT, \
        descr TEXT, \
        sensitivity_id INT, \
        is_personal BOOLEAN, \
        sensitivity_note TEXT, \
        FOREIGN KEY(sensitivity_id) REFERENCES {}(sensitivity_id)); \
        '.format(table_name, myprefix + 'sensitivity')
    create_table(conn, table_name, sql_str)

    # known_keys table
    table_name = myprefix + 'known_keys'
    sql_str = ' \
        CREATE TABLE {} ( \
        id INTEGER PRIMARY KEY AUTOINCREMENT, \
        knownkey TEXT, \
        descr TEXT, \
        pdom_id INT, \
        sensitivity_id INT, \
        is_personal BOOLEAN, \
        sensitivity_note TEXT, \
        FOREIGN KEY(sensitivity_id) REFERENCES {}(sensitivity_id)); \
        '.format(table_name, myprefix + 'sensitivity')
    create_table(conn, table_name, sql_str)

    print '-- Setup tables completed --'


def add_param_into_db(conn, myprefix, param_info):
    """
    Adds parameters into the DB as a normalised list
    Called by extract_parameters
    """

    tablename = myprefix + 'param_values'
    sql_str = ''' 
        INSERT INTO {} 
            (http_response_id, sub_domain, param_key, param_value, created_dt)
        VALUES(?,?,?,?
            ,'{}') '''.format(tablename, str(datetime.now())[:19])
    cur2 = conn.cursor()
    cur2.execute(sql_str, param_info)
    cur2.close()


def extract_parameters(conn, crawl_id, myprefix):
    """        
    Select  all data rows (with parameters)
    and split the parameters into separate parameters in a table
    """

    print '>> Extracting parameters for crawl {} <<'.format(crawl_id)
    sql_str = " \
        SELECT id as http_response_id \
            ,SUBSTR(REPLACE(REPLACE(url,'https://',''),'http://',''), \
                0,INSTR(REPLACE(REPLACE(url,'https://',''),'http://', \
                ''),'/')) as SubDomain \
            ,CASE WHEN INSTR(url,'?') > 0 \
                THEN SUBSTR(url,INSTR(url,'?'),LENGTH(url)) \
                ELSE '' \
                END as Parameters \
        FROM http_responses \
        WHERE crawl_id = {} \
            AND LENGTH(CASE WHEN INSTR(url,'?') > 0 \
                THEN SUBSTR(url,INSTR(url,'?'),LENGTH(url)) \
                ELSE '' \
                END) > 0 \
        ".format(crawl_id)
    response_count = 0
    param_count = 0
    cur = conn.cursor()
    param_list = []
    for response_id, subdomain, full_param in cur.execute(sql_str):
        response_count += 1
        full_param = full_param[1:]  # Remove leading '?'
        this_param_list = full_param.split('&')  # split parameters into a list
        for param in this_param_list:
            param_parts = param.split('=')
            if len(param_parts) == 1:
                param_parts.append('')
            param_list.append([response_id, subdomain, param_parts[0], param_parts[1]])
            param_count += 1
    print "Responses split for {} response rows".format(response_count)
    print "Adding into detailed parameter table ...."
    for item in param_list:
        add_param_into_db(conn, myprefix, item)
    conn.commit()
    print '{} parameter values added into table [{}] \
        '.format(param_count, myprefix + 'param_values')

    # show summary of data collected for each site
    sql_str = '''
        SELECT COUNT(1) as TotParams
            ,COUNT(DISTINCT sub_domain) as 'ParamSubDomains'
            ,top_url as Site
        FROM http_responses resp
            INNER JOIN {}param_values param 
                ON param.http_response_id = resp.id
        WHERE crawl_id = {}
        GROUP BY top_url 
        '''.format(myprefix, crawl_id)
    cur.execute(sql_str)
    column_names = [cn[0] for cn in cur.description]
    print "%s %s %s" % (column_names[0], column_names[1], column_names[2])
    data_rows = cur.fetchall()
    for data_row in data_rows:
        print "%7s %10s        %s" % data_row
    cur.close()

    print 'extract_parameters completed for crawl_id {}'.format(crawl_id)

