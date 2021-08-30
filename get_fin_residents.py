"""This script is reading in user names from a postgresql database"""
import db_connection as db_con
import psycopg2
import pandas as pd


def get_fin_res(country):

    # Create connection
    conn = db_con.db_engine.raw_connection()

    # Initialize cursor
    cur = conn.cursor()

    sql_query = f"SELECT userid FROM finest_residence WHERE residence_country = '{country}'" 
    data = db_con.read_sql_inmem_uncompressed(sql_query, db_con.db_engine)
    return data
    # Close connection
    cur.close()
    conn.close()
