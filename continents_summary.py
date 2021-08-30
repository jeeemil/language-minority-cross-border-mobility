
"""Small script helped to create the continents summary in master's thesis"""
import pandas as pd
import geopandas as gpd
from pyproj import CRS
import numpy as np
import matplotlib.pyplot as plt


langs = ['et', 'ru', 'sv', 'fi']

user_continents = pd.DataFrame(columns=langs)

for lang in langs:
    data = pd.read_pickle(f'{lang}_all_cb_movements.pkl')
    continents = pd.read_csv(r'outputs/all_countries_and_continents.csv', sep=';')
    
    merged_dest = pd.merge(data, continents, left_on='dest_country', right_on='countries')
    merged_orig = pd.merge(data, continents, left_on='orig_country', right_on='countries')

    all_data = merged_dest.append(merged_orig).reset_index()
    grouped_users = all_data.groupby('user_id')

    for key, group in grouped_users:

        continents = set(group.dest_country.unique())
        continents2 = set(group.orig_country.unique())
        all_continents = continents|continents2 
        unique_continents = len(all_continents) -1
        user_continents = user_continents.append({f'{lang}':unique_continents}, ignore_index=True)

user_continents.to_csv(r'outputs/all_unique_continents.csv', sep=';')