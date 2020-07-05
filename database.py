#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
Created on Sun July 05 20:00:00 2020

@author: Pratik Barjatiya
"""

import os
import sqlite3
from sqlite3 import Error
from sqlalchemy import create_engine
import pandas as pd

import yaml


def load_config():
    """
    To load execution configuration
    :return: conf
    """
    try:
        print("Reading execution config ....")
        with open(os.getcwd()+'/config/config.yml', 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
        return conf
    except Exception as e:
        print('Reading execution config failed: ')
        print("Error: {0}".format(str(e)))


def create_connection(db_file):
    """
    create a database connection to a SQLite database
    :param db_file:
    :return:
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def select_all_tasks(conn, table):
    """
    Query all rows in the tasks table
    :param table:
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    query = "SELECT * FROM "+str(table)+" limit 10"
    print(query)
    cur.execute(query)

    rows = cur.fetchall()

    for row in rows:
        print(row)


if __name__ == "__main__":
    config = load_config()
    loaded_config = config.get("TEST_CONFIG")
    destination_bucket = loaded_config.get('destination_bucket')
    book_file = loaded_config.get('book_file')
    category_file = loaded_config.get('category_file')
    database = loaded_config.get('database_file')
    print(database)

    cwd = os.getcwd()
    print(cwd)

    sql_create_books_table = """ CREATE TABLE IF NOT EXISTS books_table (
                                                id integer PRIMARY KEY,
                                                Name varchar2(50) NOT NULL,
                                                ImageLink varchar2(200) NOT NULL,
                                                Rating varchar2(50),
                                                Price varchar2(50),
                                                InStock varchar2(50),
                                                Category varchar2(50)
                                            ); """

    sql_create_category_table = """ CREATE TABLE IF NOT EXISTS category_table (
                                                    id integer PRIMARY KEY,
                                                    Name varchar2(50) NOT NULL,
                                                    Link varchar2(200) NOT NULL
                                                ); """
    db = cwd + '/' + database
    # create a database connection
    conn = create_connection(db)
    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_books_table)
        create_table(conn, sql_create_category_table)
    else:
        print("Error! cannot create the database connection.")

    engine = create_engine('sqlite:///'+cwd+'/'+database)

    books_data = pd.read_csv(cwd+'/output/books.csv')
    category_data = pd.read_csv(cwd + '/output/category.csv')

    books_data.to_sql(con=engine, name='books_table', if_exists='replace')
    category_data.to_sql(con=engine, name='category_table', if_exists='replace')

    with conn:
        print("Query all tasks")
        select_all_tasks(conn, 'books_table')
        select_all_tasks(conn, 'category_table')
