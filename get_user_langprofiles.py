
"""
Created on Tue May  5 15:19:55 2020
INFO
####
This script reads language detected tweets from a PostgreSQL database table to
a pandas dataframe and saves it locally to disk as a pickled dataframe. 
USAGE
#####
Run the script with the following command:
    python get_user_langprofiles.py -ho your.host.com -db databasename
    -u username -pw password -tb table -o path/to/tweets.pkl
#NOTE
####
The output pickle contains language detections per user in a python list.
@author: Tuomas Väisänen
"""

import pandas as pd
import psycopg2
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
import db_connection as db_con
from tqdm import tqdm


# Assign arguments to variables
database = ''
host = ''
user = ''
pw = ''

# list of probability values 0.05 to 1.0
#list_pro = [round(x * 0.05, 2) for x in range(1, 21)]
list_pro = [0.1]

# Create connection
conn = db_con.db_engine.raw_connection()
db_engine = db_con.db_engine
# Initialize cursor
cur = conn.cursor()
# Init Metadata
meta = MetaData()

# Create session
print("[INFO] - Launching database session...")
Session = sessionmaker(db_engine)
session = Session()

for prob in tqdm(list_pro):
# sql to get user language uses
    sql = f"SELECT user_id, string_agg(language::character varying,';') as langs FROM fin_res_all_tweets_for_lang WHERE prob >= {prob} GROUP BY user_id"

# retrieve data
    print("[INFO] - Querying to dataframe...")
    df = pd.read_sql(sql, con=conn)

    # convert languages to list
    df['langs'] = df['langs'].apply(lambda x: x.split(';'))

    # empty  columns
    df['fi'] = None
    df['en'] = None
    df['sv'] = None
    df['ru'] = None
    df['et'] = None

    # iterate over each tweet, count the share of tweets in each language
    for i, row in df.iterrows():
        fi = row['langs'].count('fi')
        en = row['langs'].count('en')
        sv = row['langs'].count('sv')
        ru = row['langs'].count('ru')
        et = row['langs'].count('et')

        df.loc[i, 'fi'] = fi/len(row['langs'])
        df.loc[i, 'en'] = en/len(row['langs'])
        df.loc[i, 'sv'] = sv/len(row['langs'])
        df.loc[i, 'ru'] = ru/len(row['langs'])
        df.loc[i, 'et'] = et/len(row['langs'])

    # save to csv
    print("[INFO] - Saving to disk...")
    df.to_pickle('output_' + str(prob))
    print(df)


# Close connection
cur.close()
conn.close()   
print(df)
print(df.head(5))