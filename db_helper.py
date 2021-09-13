import sqlite3
from sqlite3 import Error
import pandas as pd


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)


def create_table(conn, table_name, schema):
    sql = f""" CREATE TABLE IF NOT EXISTS {table_name} ({schema}); """
    
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def insert_data(df, table_name, conn):
    print(table_name)
    try:
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
    except Error as e:
        print(e)


def check_url(url, conn, source=True):
    """Check if a given URL already exists in webpage table
       Source_URL by default but can search by URL by setting source to False
    """

    if source:
        sql=f"""SELECT * FROM webpage WHERE source_url == '{url}' """
    else:
        sql=f"""SELECT * FROM webpage WHERE url == '{url}' """

    c = conn.cursor()
    c.execute(sql)
    rows = c.fetchall()
    
    if len(rows) >= 1:
        return True
    else:
        return False
