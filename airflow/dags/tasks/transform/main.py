import os
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error
import io
from collections import defaultdict
from airflow.decorators import task
import datetime
import math

@task
def transform(timestamp: int):

    minio_url = "minio:9000"
    print(f"Connecting to minio on {minio_url}")

    minio_client = Minio(
        minio_url,
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "data"
    output_bucket_name = "output"

    if not minio_client.bucket_exists(output_bucket_name):
        minio_client.make_bucket(output_bucket_name)

    final_df = pd.DataFrame()

    objects = minio_client.list_objects(bucket_name, recursive=True)

    folders = defaultdict(list)
    for obj in objects:
        folder = obj.object_name.split('/')[0]
        folders[folder].append(obj.object_name)

    for folder, files in folders.items():
        if folder == ".DS_Store":
            continue

        print(f"Processing folder: {folder}")
        
        station_status_path = f"{folder}/station_status.json"
        if station_status_path in files:
            station_status_response = minio_client.get_object(bucket_name, station_status_path)
            data = json.loads(station_status_response.read())

            df = pd.json_normalize(data['data']['stations'])
            df_meteo = pd.json_normalize(data['weather'])
        else:
            continue

        station_information_path = f"{folder}/station_information.json"
        if station_information_path in files:
            station_information_response = minio_client.get_object(bucket_name, station_information_path)
            data_info = json.loads(station_information_response.read())
            df_informations = pd.json_normalize(data_info['data']['stations'])
        else:
            continue

        cleanup = {'weather': {'Ensoleillé': 0, 'Nuageux': 1, 'Peu nuageux': 1, 'Très nuageux': 1, 'Pluie': 2, 'Averses': 2, 'Neige': 3, 'Brouillard': 4, 'Orage': 5, 'Couvert': 6, 'Ciel voilé': 7}}
        df_meteo.replace(cleanup, inplace=True)

        df['id'] = data['lastUpdatedOther']

        df['mechanical'] = df['num_bikes_available_types'].apply(lambda x: x[0]['mechanical'])
        df['ebike'] = df['num_bikes_available_types'].apply(lambda x: x[1]['ebike'])
        df.drop(['num_bikes_available_types'], axis=1, inplace=True)

        df = pd.merge(df, df_meteo, how='cross')

        df = pd.merge(df, df_informations[['station_id', 'lon', 'lat', 'name']], on='station_id', how='left')

        df.drop(['numBikesAvailable', 'numDocksAvailable'], axis=1, inplace=True)

        df = df[['id', 'stationCode', 'station_id', 'num_bikes_available', 'mechanical', 'ebike', 'num_docks_available', 'is_installed', 'is_returning', 'is_renting', 'last_reported', 'weather', 'temp', 'probarain', 'probafog', 'probawind70', 'probawind100', 'lon', 'lat', 'name']]

        df = df[df['stationCode'].notna()]

        if df['stationCode'].dtype == 'int64':
            df['stationCode'] = df['stationCode'].astype('int64')

        final_df = pd.concat([final_df, df], axis=0)

    csv_buffer = io.BytesIO()
    final_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    minio_client.put_object(
        output_bucket_name,
        f"{timestamp}.csv",
        data=csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type='application/csv'
    )

    print("Data successfully processed and saved to MinIO bucket 'output'.")
    return timestamp
