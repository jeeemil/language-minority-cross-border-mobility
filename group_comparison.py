import pandas as pd
from statistics import mean

langs = ['et', 'ru', 'sv', 'fi']
user_countries = pd.DataFrame(columns=['user_id', 'lang', 'unique_countries'])

# Looping through cross-border movements
for lang in langs:
    data = pd.read_pickle(f'{lang}_all_cb_movements.pkl')
    # Dropping all movements not related to Finland
    data = data.dropna()

    # Grouping by user
    grouped_users = data.groupby('user_id')
    # Iterate over each user's tweets
    for key, group in grouped_users:
        # get all unique countries
        countries = set(group.dest_country.unique())
        countries2 = set(group.orig_country.unique())
        all_countries = countries|countries2
        # Delete one (Finland) from number of unique countries
        unique_countries = len(all_countries) - 1
        # append to dataframe
        user_countries = user_countries.append({'user_id':key, 'lang':f'{lang}', 'unique_countries':unique_countries}, ignore_index=True)

# Setting number of countries to integer and setting lang_code column
user_countries['unique_countries'] = user_countries['unique_countries'].astype(int)
user_countries['lang_code'] = user_countries['lang']
user_countries['lang_code'].replace({'et':1, 'ru':2, 'sv':3, 'fi':4}, inplace=True)

# Dataframe for municipalities
user_municipalities = pd.DataFrame(columns=['user_id', 'unique_municipalities', 'km_mean'])

for lang in langs:
    data = pd.read_pickle(f'{lang}_municipality_lines.pkl')
    data = data.dropna()
    # Iteratating through each user
    grouped_users = data.groupby('user_id')

    for key, group in grouped_users:
        # Collecting number of unique municipalities
        municipalities = set(group.dest_mun.unique())
        municipalities2 = set(group.orig_mun.unique())
        all_municipalities = municipalities|municipalities2
        unique_municipalities = len(all_municipalities) - 1
        # kilometer mean for user
        km_mean = mean(group['distance_km'])
        user_municipalities = user_municipalities.append({'user_id':key, 'unique_municipalities':unique_municipalities, 'km_mean': km_mean}, ignore_index=True)


# Setting number of unique municipalities to integer
user_municipalities['unique_municipalities'] = user_municipalities['unique_municipalities'].astype(int)

# Merging user country data and municipal data
merged = pd.merge(user_countries, user_municipalities, on='user_id')
merged[['unique_countries', 'lang_code', 'unique_municipalities']] = merged[['unique_countries', 'lang_code', 'unique_municipalities']].astype(int)


all_index = pd.DataFrame(columns=['user_id', 'shannon', 'simpson'])

for lang in langs:
    data = pd.read_csv(f'outputs/user with shannon/{lang}_index_table_new.csv')
    for i, row in data.iterrows():
        data = data.dropna()
        all_index = all_index.append({'user_id':row['user_id'], 'shannon':row['shannon'], 'simpson':row['simpson']},ignore_index=True)


index_merge = pd.merge(merged, all_index, on='user_id', how='inner')
index_merge = index_merge.reset_index()


#mega_merge = pd.merge(index_merge, share_data, on='user_id', how='inner')
#mega_merge.to_csv('outputs/every_single_variables.csv')


share_data = pd.read_pickle(r'outputs/all_lang_share.pkl')
mega_merge = pd.merge(index_merge, share_data, on='user_id', how='left')
mega_merge.to_csv('outputs/every_single_variables.csv')

"""import db_connection as db_con
import psycopg2

# Create connection
conn = db_con.db_engine.raw_connection()    
# Initialize cursor
cur = conn.cursor()
index_merge.to_sql('group_comparison_variables',con=db_con.db_engine)
"""

#all_data.to_csv(r'outputs/group_comparison.csv')
mega_merge.to_pickle(r'outputs/group_comparison.pkl')
