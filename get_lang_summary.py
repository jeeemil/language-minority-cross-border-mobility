"""This script is based on and built upon the works of Tuomas Väisänen 
(https://github.com/DigitalGeographyLab/maphel-finlang/blob/master/get_user_langprofiles.py)
This script reads a database that contains tweets with an identified language.
It returns a summary table of a certain set probability
"""

import pandas as pd
import psycopg2
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
import db_connection as db_con

# list of probability values 0.05 to 1.0
#list_pro = [round(x * 0.05, 2) for x in range(1, 21)] 
list_pro = [0.7]
lang_list = ['fi','sv','ru','et', 'en']


# Create connection
conn = db_con.db_engine.raw_connection()
db_engine = db_con.db_engine
# Initialize cursor
cur = conn.cursor()
# Init Metadata
meta = MetaData()

# Create session
print("Launching database session...")
Session = sessionmaker(db_engine)
session = Session()

# Empty dataframe
data_2 = pd.DataFrame()

# Looping through each probability
for prob in list_pro:
# Query for one user, fetching all languages used with a certain probability, grouping by user
    sql = f"SELECT user_id, string_agg(language::character varying,';') as langs FROM fin_res_all_tweets_for_lang WHERE prob >= {prob} GROUP BY user_id"

    # Retrieve data
    print("Querying dataframe with all tweets with a language probability of " + str(prob))
    data = pd.read_sql(sql, con=conn)

    # Convert languages to list
    data['langs'] = data['langs'].apply(lambda x: x.split(';'))

    # Iterate through list of languagues
    for lang in lang_list:
        
        # Calculate the share of a particular language
        
        data_2[str(prob) + '_' + str(lang)] = data['langs'].apply(lambda row: row.count(lang)/len(row))
        #data_2[str(prob) + '_' + str(lang)]


    

# Close connection
cur.close()
conn.close()  

total_number_of_users = len(data)   
# Create empty list to store all summed values
data_list = []

# Save all probabilities per language into list
for prob in list_pro:
    for lang in lang_list:
        data_list.append(len(data_2[data_2[str(prob) + '_' + str(lang)]>=prob]))

print(data_list)
print(len(data_list))
# Insert values in dataframe
sum_data = pd.DataFrame([data_list], columns = data_2.columns)

# Create dataframe for percentages
sum_data_pro = pd.DataFrame(index=lang_list, columns=list_pro)

for prob in list_pro:
    for lang in lang_list:
        sum_data_pro.loc[str(lang), prob] = sum_data.loc[0, str(prob) + '_' + str(lang)]/total_number_of_users

# Uncomment next row to save csv file
#sum_data_pro.to_csv('user_lang_sum_pro.csv')
print(sum_data_pro)