#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
####
This script is based upon the script: https://github.com/DigitalGeographyLab/maphel-finlang/blob/master/twitter_multilangid.py
from the maphel-finlang repository: https://github.com/DigitalGeographyLab/maphel-finlang
This is a modified version of the work of Tuomas Väisänen and Tuomo Hiippala
"""

import pandas as pd
import psycopg2
import psycopg2.extras as extras
import fasttext
import operator
import numpy as np
import geopandas as gpd
import json
from urllib.parse import urlparse
import emoji
import re
import io
from io import StringIO
import tempfile
import os
import csv
from sqlalchemy import create_engine, func, distinct
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from nltk.tokenize.punkt import PunktSentenceTokenizer
import langcodes



import sys
sys.path.insert(0,'../')
import db_connection as db_con



# Define the preprocessing function from Hiippala et al. 2019
def preprocess_caption(row: str, mode: str) -> str:
    """Applies the selected preprocessing steps to the text.
     Args:
         row: A UTF-8 string.
         mode: A string indicating the selected preprocessing strategy.
               Valid values include: 'no_preprocessing' (no preprocessing),
               'rm_all' (remove all hashtags) and 'rm_trail' (remove trailing
               hashtags).
     Returns:
         A string containing the preprocessed text.
    """
    # Check if preprocessing has been requested.
    if mode != 'no_preprocessing':

        # Convert unicode emoji to shortcode emoji
        try:
            row = emoji.demojize(row)
        except TypeError:
            pass

        # Remove single emojis and their groups
        row = re.sub(r':(?<=:)([a-zA-Z0-9_\-&\'’]*)(?=:):', '', row)

        # Apply the selected preprocessing strategy defined in the variable
        # 'mode'. This defines how each row is processed. The selected mode
        # defines the preprocessing steps applied to the data below by
        # introducing different conditions.

        # Remove all mentions (@) in the caption
        row = re.sub(r'@\S+ *', '', row)

        # If mode is 'rm_all', remove all hashtags (#) in the caption
        #if mode == 'rm_all':
        #    row = re.sub(r'#\S+ *', '', row)
        
        # remove hashes from hastags
        row = row.replace('#', '')
        
        # remove old school heart emojis <3
        row = row.replace('&lt;3', '')
        
        # remove greater than symbols >
        row = row.replace('&gt;', '')

        # Split the string into a list
        row = row.split()

        # Remove all non-words such as smileys etc. :-)
        row = [word for word in row if re.sub('\W', '', word)]

        # Check the list of items for URLs and remove them
        try:
            row = [word for word in row if not urlparse(word).scheme]
        except ValueError:
            pass

        # Attempt to strip extra linebreaks following any list item
        row = [word.rstrip() for word in row]

        # If mode is 'rm_trail', remove hashtags trailing the text, e.g.
        # "This is the caption and here are #my #hashtags"
        if mode == 'rm_trail':
            while len(row) != 0 and row[-1].startswith('#'):
                row.pop()

        # Reconstruct the row
        row = ' '.join(row)

        # If mode is 'rm_trail', drop hashes from any remaining hashtags
        if mode == 'rm_trail':
            row = re.sub(r'g*#', '', row)

    # Simplify punctuation, removing sequences of exclamation and question
    # marks, commas and full stops, saving only the final character
    row = re.sub(r'[?.!,_]+(?=[?.!,_])', '', row)

    # Return the preprocessed row
    return row

# function to split sentences from Hiippala et al. 2019
def split_sentence(caption):
    """Tokenizes sentences using NLTK's Punkt tokenizer.
    Args:
        caption: A string containing UTF-8 encoded text.
    Returns:
        A list of tokens (sentences).
    """

    # Initialize the sentence tokenizer
    tokenizer = PunktSentenceTokenizer()

    # Tokenize the caption
    sent_tokens = tokenizer.tokenize(caption)

    # Return a list of tokens (sentences)
    return sent_tokens


# function to detect language with fastText from Hiippala et al. 2019
def detect_ft(caption, preprocessing):
    """Identifies the language of a text using fastText.
    Args:
        caption: A string containing UTF-8 encoded text.
        preprocessing: A string indicating the selected preprocessing strategy.
                       Valid values include: 'no_preprocessing'
                       (no preprocessing),  'rm_all' (remove all hashtags) and
                       'rm_trail' (remove trailing hashtags).
    Returns:
        Saves the prediction into a column named 'langid' in the pandas
        DataFrame as a list of three tuples. The three tuple consists of an
        ISO-639 code, its associated probability and character length of the
        string input to fastText, e.g. ('en', 0.99999, 21).
    """
    # If the caption is None, return None
    if caption == 'None' or caption is None:
        return

    # Preprocess the caption
    caption = preprocess_caption(caption, preprocessing)

    # Perform sentence splitting for any remaining text
    if len(caption) == 0:
        return None

    else:
        # Calculate the character length of each sentence
        char_len = len(caption)

        # Make predictions
        prediction = ft_model.predict(caption)

        # Get the predicted languages and their probabilities
        language = prediction[0][0].split('__')[-1]
        probability = prediction[1][0]
        #print(language)
        # Return languages and probabilities
        return [language, probability, char_len]


def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table.
    Change to lat, lon etc if using other table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False)
    buffer.seek(0)
    cursor = conn.cursor()
    cursor.execute("CREATE TEMP TABLE tmp_table (id BIGINT, row_id BIGINT, language VARCHAR(10),lang_name VARCHAR(50), prob NUMERIC, charlen NUMERIC);")
    
    
    try:
        cursor.copy_from(buffer, "tmp_table", sep=",")
        cursor.execute(f"UPDATE {table} SET language = data.language, lang_name = data.lang_name, prob = data.prob, charlen = data.charlen FROM tmp_table AS data WHERE {table}.row_id = data.row_id;")
        cursor.execute("DROP TABLE tmp_table")
        cursor.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    #cursor.close()


# Select table to update
table = 'fin_res_for_lang'
# Create connection
conn = db_con.db_engine.raw_connection()
# Initialize cursor
cur = conn.cursor()
# SQL statement
findCountSql = f"SELECT row_id FROM {table} ORDER BY row_id DESC LIMIT 1"
# Execute sql query
findCount = db_con.read_sql_inmem_uncompressed(findCountSql, db_con.db_engine)
# Find the last tweet
last_tweet = findCount['row_id'][0]
# Close connection
cur.close()
conn.close()
print(f"{last_tweet} number of records")

# Update parameters
batch_size = 100000
start_number = 0
max_number = 100000

percent = 0
percent_per_round = round((batch_size / last_tweet) * 100,2)
# Initialize connection
conn = db_con.psyco_con
#cursor = conn.cursor()


# Model settings:
prep = 'rm_all'
ft_model = fasttext.load_model("lid.176.bin")

for offset in range(0, last_tweet, batch_size):
    print('Starting')
    # SQL query
    sql_query = f"SELECT user_id, created_at, user_name, user_realname, user_loc, user_descr, text1, country, row_id FROM {table} WHERE row_id <{max_number} AND row_id>={start_number} ORDER BY row_id ASC LIMIT {batch_size}"
    # Read results to dataframe
    df = db_con.read_sql_inmem_uncompressed(sql_query, db_con.db_engine)
    print('dataframe read')
    # Extract data
    # detect languages
    print('[INFO] - Detecting languages')
    df['langid'] = df['text1'].apply(lambda x: detect_ft(x, prep))
    # parse results
    print('[INFO] - Parsing results to improve readability')

    df['language'] = df['langid'].apply(lambda x: x[0] if x != None else None)
    print('[INFO] - Language ISO conversion')
    df['lang_name'] = df['langid'].apply(lambda x: langcodes.get(x[0]).display_name('en') if x != None  else None)
    df['prob'] = df['langid'].apply(lambda x: x[1] if x != None else 'NaN')
    df['charlen'] = df['langid'].apply(lambda x: x[2] if x != None else 'NaN')
    df = df[['row_id', 'language','lang_name','prob','charlen']]
    # Push data to db
    copy_from_stringio(conn, df, table)

    # Delete temporary dataframe
    del df
    start_number += batch_size
    max_number += batch_size
    percent += percent_per_round
    print(f'{round(percent,2)}% done.')


conn.commit()
conn.close()