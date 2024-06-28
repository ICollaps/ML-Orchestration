# Native libraries
import os
import math
import operator
import json
# Essential Libraries
import requests
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from tqdm import tqdm
# Preprocessing
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
# Algorithms
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, SimpleRNN
from sklearn.metrics import f1_score
import warnings

warnings.simplefilter("ignore")



def getMeteo():
    response = requests.get(
        "https://api.meteo-concept.com/api/forecast/nextHours?token=beb09bde46829759e48f6158a0f8c1e08c3b1e62448e8529b6d162a4216849b3&insee=75056&hourly=true")
    data = response.json()
    return data

def getVelibStationStatus():

    # Get the data from the API
    data = pd.read_csv('250624-bis.csv')

    return data

def generateCallFormated():

    final_df = pd.DataFrame()

    velibdata = getVelibStationStatus()
    meteo = getMeteo()

    # Create pandas dataframe from the call to the API
    df = velibdata
    df_meteo = pd.json_normalize(meteo['forecast'])

    # Clean the weather value:
    cleanup = {'weather': {'Ensoleillé': 0,
                           'Soleil': 0,
                           'Nuageux': 1,
                           'Peu nuageux': 1,
                           'Très nuageux': 1,
                           'Pluie': 2,
                           'Pluies': 2,
                           'Averses': 2,
                           'Neige': 3,
                           'Brouillard': 4,
                           'Orage': 5,
                           'Orages': 5,
                           'Couvert': 6,
                           'Ciel voilé': 7,
                           }}

    df_meteo.replace(cleanup, inplace=True)

    # Add the id (timestamp) to the dataframe and change the order of the columns
    df['id'] = velibdata['id']

    '''temp_cols = df.columns.tolist()
    index = temp_cols.index('id')
    new_cols = temp_cols[index:index+1] + temp_cols[0:index] + temp_cols[index+1:]
    df = df[new_cols]
    temp_cols'''

    df.drop(['weather', 'probarain', 'probafog', 'probawind70', 'probawind100'], axis=1, inplace=True)

    # Add the weather data to the dataframe
    df = pd.merge(df, df_meteo, how='cross')

    # DF PA
    df = df[['id', 'stationCode', 'station_id', 'num_bikes_available', 'mechanical', 'ebike', 'num_docks_available',
             'is_installed', 'is_returning', 'is_renting', 'last_reported', 'weather', 'temp2m', 'probarain',
             'probafog', 'probawind70', 'probawind100']]

    # Delete NoneType values in the dataframe on the column 'stationCode'
    df = df[df['stationCode'].notna()]

    # Problem with a type of station with "relai" in the stationCode column
    if df['stationCode'].dtype == 'int64':
        df['stationCode'] = df['stationCode'].astype('int64')
    else:
        pass

    final_df = pd.concat([final_df, df], axis=0)

    df_unique_stations = final_df.drop_duplicates(subset='stationCode', keep='first')

    return df_unique_stations

# Lire API
df_velib = generateCallFormated()
df_velib.rename(columns={'temp2m': 'temp'}, inplace=True)
df_velib.head()

def convert_epoch_time_day_hour(epoch_time):
    return datetime.fromtimestamp(epoch_time).strftime('%A %H:%M')

def convert_epoch_time(epoch_time):
    return datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')

def data_prep(df_velib):
    df_velib['stationCode'] = df_velib['stationCode'].astype(str)
    df_velib.drop('is_returning', axis=1, inplace=True)  # redundant, since same values as column 'is_renting'
    list_dates = []
    list_days = []

    for i in df_velib['id'].tolist():
        list_dates.append(convert_epoch_time(i))
        list_days.append(convert_epoch_time_day_hour(i))

    df_velib['date_time'] = pd.to_datetime(list_dates)
    df_velib['day_time'] = list_days

    return df_velib

df_velib = data_prep(df_velib)

df_velib['weekend'] = df_velib['day_time'].str.contains('Saturday|Sunday').astype(int)

df_velib['date_time'] = pd.to_datetime(df_velib['date_time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

df_velib['date'] = df_velib['date_time'].dt.date
df_velib['date'] = pd.to_datetime(df_velib['date'], format='%Y-%m-%d')
df_velib['time'] = df_velib['date_time'].dt.time
df_velib['hour'] = df_velib['date_time'].dt.hour
df_velib['minute'] = df_velib['date_time'].dt.minute
# df_velib.head()

df_jours_ferie = pd.read_csv('jours_feries_metropole.csv')
df_jours_ferie['date'] = pd.to_datetime(df_jours_ferie['date'], format='%Y-%m-%d')

df_velib = pd.merge(df_velib, df_jours_ferie, how='left', on='date')
df_velib.drop(['annee', 'zone'], axis=1, inplace=True)
df_velib['jour_ferie'] = df_velib['nom_jour_ferie'].notna().astype(int)
df_velib.drop(['nom_jour_ferie'], axis=1, inplace=True)

# rajouter latitude et longitude
with open("station_information.json") as json_file:
    data = json.load(json_file)
df_station_info = pd.json_normalize(data['data'], "stations")
df_station_info['stationCode'] = df_station_info['stationCode'].astype(str)
df_station_info = df_station_info[['stationCode', 'name', 'capacity', 'lat', 'lon']]
df_velib = pd.merge(df_velib, df_station_info, how='left', on='stationCode')

futur_epochs = [(5, 300), (10, 600), (15, 900), (20, 1200), (25, 1500), (30, 1800)]
dfs_velib = [df_velib] * len(futur_epochs)

# Create a dictionary mapping the combination of 'id' and 'stationCode' to the corresponding 'num_docks_available'
availability_dict = df_velib.set_index(['stationCode', 'id']).groupby(level=[0, 1])[
    'num_docks_available'].first().to_dict()
sorted_epochs = sorted(set(df_velib['id']))

for i in range(len(futur_epochs)):
    df_velib_fut = dfs_velib[i].copy()
    # on rajoute une feature qui indique le moment dans le futur pour lequel on veut prédire les disponibilités
    df_velib_fut['futur_min'] = futur_epochs[i][0]
    # On rajoute 5 minutes aux timestamps (= 300 epochs)
    df_velib_fut['futur_epoch'] = df_velib_fut['id'] + futur_epochs[i][1]

    df_tuples = list(zip(df_velib_fut['stationCode'].tolist(), df_velib_fut['futur_epoch'].tolist()))

    futur_availability = []

    for tuple in tqdm(df_tuples):
        avail = availability_dict.get(tuple)
        if avail == None:
            later_timestamps = [timestamp for timestamp in sorted_epochs if timestamp >= tuple[1]]
            # if there exists availability data after the current epoch + 5 min, we take the next timestamp we can find
            # else, avail keeps the value None
            if len(later_timestamps) != 0:
                avail = availability_dict.get((tuple[0], later_timestamps[0]))
        futur_availability.append(avail)

    df_velib_fut['futur_availability'] = futur_availability

    dfs_velib[i] = df_velib_fut

df_velib_futur = pd.concat(dfs_velib, axis=0)

df_cluster_station = pd.read_csv('station_cluster.csv')
df_cluster_station['stationCode'] = df_cluster_station['stationCode'].astype(str)
df_velib_futur = pd.merge(df_velib_futur, df_cluster_station, how='left', on='stationCode')

df_velib_futur = df_velib_futur[~df_velib_futur['Cluster'].isna()]

df_velib_training = df_velib_futur[
    ['stationCode', 'Cluster', 'num_docks_available', 'capacity', 'hour', 'minute', 'temp', 'probarain', 'weekend',
     'jour_ferie', 'lat', 'lon', 'futur_min', 'futur_availability']]

df_velib = df_velib_training
csv_path = 'data.csv'

df_velib.to_csv(csv_path, index=False)

print(f"DataFrame exported to {csv_path}")