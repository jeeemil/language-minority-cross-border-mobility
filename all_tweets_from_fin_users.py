"""This script is reading in user names 
and choosing the columns needed for language recognition
"""
import db_connection as db_con
import psycopg2
import pandas as pd
from tqdm import tqdm
import get_fin_residents

# read in users and making it a list
users = get_fin_residents.get_fin_res('Finland')
user_list = users.userid.tolist()

# Specify the name of the new table to be put to the database as a string
new_table = 'fin_res_all_tweets_for_lang'

# Create connection
conn = db_con.db_engine.raw_connection()
# Initialize cursor
cur = conn.cursor()

# Specify datatypes for the data from the database
data_types_dict = {'user_name': str, 'user_realname': str, 'user_loc': str, 'user_descr': str, 'text1': str}

# for loop over users. Using tqdm to track progress.
for user in tqdm(user_list):
    # sql query for user
    sql_query = f"SELECT user_id, created_at, user_name, user_realname, user_loc, user_descr, text1, row_id FROM twitter_histories_joined_finest WHERE user_id = {user}"
    # executing query
    data = db_con.read_sql_inmem_uncompressed(sql_query, db_con.db_engine)
    # Change the data types of the dataframe (to ensure that the upload to database goes well)
    data = data.astype(data_types_dict)
    # Uploading the dataframe to the database
    data.to_sql(new_table,con=db_con.db_engine, if_exists='append')
    
# Close connection
cur.close()
conn.close()