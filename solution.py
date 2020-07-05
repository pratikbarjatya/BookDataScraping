#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
Created on Sun July 05 20:00:00 2020

@author: Pratik Barjatiya
"""

import re
import os
from io import StringIO
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
import boto3
import sqlite3
from sqlite3 import Error


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
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


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


def get_and_parse_url(param):
    """
    To fetch and parse urls
    :param param:
    :type param: url object
    :rtype: object
    :return: soup: parsed html object
    """
    result = requests.get(param)
    soup = BeautifulSoup(result.text, 'html.parser')
    return soup


def get_books_url(param):
    """
    To fetch urls
    :param param:
    :type param: url object
    :return: parsed url object
    """
    soup = get_and_parse_url(param)
    # remove the index.html part of the base url before returning the results
    return (
        ["/".join(param.split("/")[:-1]) + "/" + x.div.a.get('href') for x in
         soup.findAll("article", class_="product_pod")])


def clean_category_name(category_name):
    """
    To clean category names
    :param category_name:
    :return: cleaned category names
    """
    return category_name.split('_')[0]


def _write_df_to_csv_on_s3(dataframe, filename):
    """
    Write a data-frame to a CSV on S3 Object Stores
    :param dataframe: dataframe
    :param filename:
    :return:
    """

    print("Writing {} records to {}".format(len(dataframe), filename))
    # Create buffer
    csv_buffer = StringIO()
    # Write data-frame to buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # Create S3 object
    s3_resource = boto3.resource("s3")
    # Write buffer to S3 object
    s3_resource.Object(destination_bucket, filename).put(Body=csv_buffer.getvalue())


if __name__ == "__main__":
    config = load_config()
    loaded_config = config.get("TEST_CONFIG")
    destination_bucket = loaded_config.get('destination_bucket')
    main_url = loaded_config.get('main_url')
    new_page = loaded_config.get('new_page')
    book_file = loaded_config.get('book_file')
    category_file = loaded_config.get('category_file')
    database = loaded_config.get('database_file')
    # tables_df = pd.DataFrame()

    cwd = os.getcwd()
    books_path = os.path.join(cwd, "output", book_file)
    category_path = os.path.join(cwd, "output", category_file)

    result = requests.get(main_url)
    soup = BeautifulSoup(result.text, 'html.parser')
    print(soup.prettify()[:1000])

    main_page_products_urls = [x.div.a.get('href') for x in soup.findAll("article", class_="product_pod")]
    print(str(len(main_page_products_urls)) + " fetched products URLs")

    categories_urls = [main_url + x.get('href') for x in
                       soup.find_all("a", href=re.compile("catalogue/category/books"))]
    categories_urls = categories_urls[1:]  # we remove the first one because it corresponds to all the books
    print(str(len(categories_urls)) + " fetched categories URLs")

    pages_urls = []
    while requests.get(new_page).status_code == 200:
        pages_urls.append(new_page)
        new_page = pages_urls[-1].split("-")[0] + "-" + str(
            int(pages_urls[-1].split("-")[1].split(".")[0]) + 1) + ".html"
    print(str(len(pages_urls)) + " fetched URLs")

    booksURLs = []
    for page in pages_urls:
        booksURLs.extend(get_books_url(page))
    print(str(len(booksURLs)) + " fetched URLs")

    """**Scrape all the information**"""
    names = []
    image_link = []
    ratings = []
    prices = []
    nb_in_stock = []
    categories = []
    # scrape data for every book URL: this may take some time
    for url in booksURLs:
        soup = get_and_parse_url(url)
        # book name
        names.append(soup.find("div", class_=re.compile("product_main")).h1.text)
        # book link
        image_link.append(soup.find("div", class_="item active").img.get("src"))
        # book price
        prices.append(soup.find("p", class_="price_color").text[2:])  # get rid of the pound sign
        # book category
        categories.append(soup.find("a", href=re.compile("../category/books/")).get("href").split("/")[3])
        # book ratings
        ratings.append(soup.find("p", class_=re.compile("star-rating")).get("class")[1])
        # number of available book
        nb_in_stock.append(
            re.sub("[^0-9]", "",
                   soup.find("p", class_="instock availability").text))  # get rid of non numerical characters
    # add data into pandas df
    books_data = pd.DataFrame({'Name': names, 'Image Link': image_link, 'Rating': ratings,
                               'Price(£)': prices, 'In-Stock': nb_in_stock, 'Category': categories})
    books_data.loc[:, "Category"] = books_data["Category"].apply(clean_category_name)
    books_data.to_csv(books_path, index=False, encoding='utf-8')

    """**Scrape the data : Book Category**"""
    categories = []
    link = []

    # scrape data for every book URL: this may take some time
    for url in booksURLs:
        soup = get_and_parse_url(url)
        # product category
        categories.append(soup.find("a", href=re.compile("../category/books/")).get("href").split("/")[3])
        link.append(soup.find("a", href=re.compile("../category/books/")).get("href"))

    df_categories = pd.DataFrame({"Name": categories, "Link": link})
    df_categories = df_categories.drop_duplicates(subset="Name")
    df_categories.loc[:, "Name"] = df_categories["Name"].apply(clean_category_name)
    df_categories.sort_values(by=['Name'], inplace=True)
    df_categories.to_csv(category_path, index=False, encoding='utf-8')

    # _write_dataframe_to_csv_on_s3(tables_df, str(output_file))
