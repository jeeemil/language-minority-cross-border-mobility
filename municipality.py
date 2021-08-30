from shapely import geometry
import sqlalchemy.pool as pool
import psycopg2
import multiprocessing as mp
import db_connection as db_con
from multiprocessing import Pool
import pandas as pd
import geopandas as gpd
import io
import time 
from tqdm import tqdm
from shapely.geometry import Point, LineString
from datetime import datetime
from math import cos, sin, asin, sqrt, radians
import matplotlib.pyplot as plt
import contextily as ctx
import pickle
import numpy as np
from pyproj import CRS
import get_lang_user_list

#start_t = time.time()
# 1. DEFINE FUNCTIONS AND STUFF
# storing column names with correct datatype
data_types_dict = {'created_at': str, 'coordinates': str, 'place_name': str, 'place': str, 'id': int, 'text1': str,
       'user_id': int, 'user_name': str,
       'user_realname': str, 'user_loc': str, 'user_descr': str, 
        'row_id': int, 'spatial_level': str,
       'lat': str, 'lon': str, 'country': str}

# Create a function that returns a connection to the database
def getconn():
    c = psycopg2.connect(user=db_con.db_username, host=db_con.db_host, dbname=db_con.db_name, password=db_con.db_pwd)

    return c

def multi_read_sql_inmem_uncompressed(query, db_engine):

    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query=query, head="HEADER")
    conn = db_engine
    cur = conn.cursor()
    store = io.StringIO()
    cur.copy_expert(copy_sql, store)
    store.seek(0)
    df = pd.read_csv(store)
    cur.close()
    db_engine.close()
    return df

def calc_distance(lat1, lon1, lat2, lon2):
    
    """
    Calculate the great circle distance between two points
    on Earth (specified in decimal degrees)
    """
    
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6371 * c
    return km

def shift_userLines(user):
    # Get WFS of municipalities from Statistics Finland
    url = "http://geo.stat.fi/geoserver/tilastointialueet/wfs?request=GetFeature&typename=tilastointialueet:kunta1000k_2021&outputformat=JSON"
    municipalities = gpd.read_file(url)
    # Subsetting and changing CRS and column names
    municipalities = municipalities.to_crs(epsg=4326)
    municipalities = municipalities[['geometry', 'name']]
    municipalities = municipalities.rename(columns={'name':'orig_mun'})
    
    # Creating a geodataframe for the line data and setting CRS
    line_data = gpd.GeoDataFrame(columns=['geometry', 'user_id','orig_mun', 'dest_mun', 'orig_time' , 'dest_time',  'duration', 'distance_km'], geometry='geometry')
    line_data = line_data.set_crs(epsg=4326)

    # Establishing query, ordering by row_id
    query1 = f'SELECT id, row_id, user_id, created_at, lat, lon, country FROM twitter_histories_joined_finest WHERE user_id = {user} ORDER BY row_id'
    
    # Creating connection pool, reading a dataframe based on the query and the connection
    conn= mypool.connect()
    user1 = multi_read_sql_inmem_uncompressed(query1, conn)
    print(f"User {user} data loaded")
    
    # Creating a datetime column and renaming it. Creating a geodataframe with the coordinates
    user1['created_at'] = user1['created_at'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    
    user1 = user1.rename(columns={'created_at': 'orig_time'})
    user1 = gpd.GeoDataFrame(user1, geometry=gpd.points_from_xy(user1.lon,user1.lat), crs="epsg:4326")
    

    # Sorting values by the timestamp    
    user1 = user1.sort_values(by='orig_time')
    # Taking the tweetpoints and match them with the municipality their in
    user1 = gpd.sjoin(user1, municipalities, how='left', op='within')


    #user1['orig_mun'].fillna('Abroad', inplace=True)
    #user1[or]

    # Dropping unused columns
    user1 = user1.drop(columns=['id','index_right'])
    # create shifted columns
    user1['geo2'] = user1['geometry'].shift(-1)
    user1['dest_mun'] = user1['orig_mun'].shift(-1)
    user1['dest_time'] = user1['orig_time'].shift(-1)
    # Defining the movement locations
    #user1['move'] = user1.apply(lambda x: str(x['orig_mun'])+'-'+str(x['dest_mun']) ,axis=1)
    #print(user1)
    # Estimating the duration
    user1['duration'] = user1.apply(lambda x: (x['dest_time']-x['orig_time']).days,axis=1)
    # drop last row as last row has no value in geo2 due to shifting
    user1 = user1[:-1]
    if len(user1) <1:
        return line_data
    else:
        user1['move'] = user1.apply(lambda x: str(x['orig_mun'])+'-'+str(x['dest_mun']) ,axis=1)
        # create linestrings
        user1['line'] = user1.apply(lambda x: LineString([x['geometry'], x['geo2']]), axis=1)
        # replace point geometry with linestring
        user1['geometry'] = user1['line']
        # calculate distance
        user1['distance_km'] = user1['line'].apply(lambda line: calc_distance(line.xy[1][0], line.xy[0][0], line.xy[1][1], line.xy[0][1]))
        # clean up dataframe columns
        user1 = user1.drop(columns=['geo2','line'])
        # append individual values to line_data
        line_data = line_data.append(user1, ignore_index=True)
        line_data['orig_time'] = line_data['orig_time'].apply(lambda x: x.strftime("%Y-%m-%d-%H"))
        line_data['dest_time'] = line_data['dest_time'].apply(lambda x: x.strftime("%Y-%m-%d-%H"))
        
        return line_data



mypool = pool.QueuePool(getconn, max_overflow=5, pool_size=4)

# Define language
lang = 'fi'
#user_list = [3600902361,3677747417]
user_list = get_lang_user_list.get_user_list(lang)
nr_users = len(user_list)


if __name__=='__main__':
    print('main start')
    # Get the number of CPU's and leave one unit for keeping the computer alive
    num_processes = mp.cpu_count()-1

    a_pool = mp.Pool(num_processes)
    print('pool created')

    result = a_pool.map(shift_userLines, user_list)
    a_pool.close()
    a_pool.join()

    
    
    data = pd.DataFrame()
    data = pd.concat(result)
    
    only_cb = data[data.orig_mun != data.dest_mun]
    geo = gpd.GeoDataFrame(only_cb, geometry='geometry', crs='epsg:4326')
    geo = geo.to_crs(epsg=3857)
    
    only_cb.to_pickle('fi_municipality_lines.pkl')