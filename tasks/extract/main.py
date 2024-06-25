import json
import os
import datetime
import math
from minio import Minio
import io
from velibData import getVelibStationStatus, getVelibStationInformations
from meteo import getMeteo, convertMeteoValue

# Configuration MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "data"

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

now = datetime.datetime.now()
timestamp = math.trunc(datetime.datetime.timestamp(now))

data_velib_status = getVelibStationStatus()
data_velib_informations = getVelibStationInformations()
data_meteo = getMeteo()

data_velib_status['weather'] = {
    'weather': convertMeteoValue(data_meteo),
    'temp': data_meteo['forecast'][0]['temp2m'],
    'probarain': data_meteo['forecast'][0]['probarain'],
    'probafog': data_meteo['forecast'][0]['probafog'],
    'probawind70': data_meteo['forecast'][0]['probawind70'],
    'probawind100': data_meteo['forecast'][0]['probawind100'],
}

station_status_file = f"{timestamp}/station_status.json"
station_information_file = f"{timestamp}/station_information.json"
meteo_file = f"{timestamp}/meteo.json"

station_status_data = io.BytesIO(json.dumps(data_velib_status).encode('utf-8'))
station_information_data = io.BytesIO(json.dumps(data_velib_informations).encode('utf-8'))
meteo_data = io.BytesIO(json.dumps(data_meteo).encode('utf-8'))

minio_client.put_object(
    bucket_name,
    station_status_file,
    data=station_status_data,
    length=station_status_data.getbuffer().nbytes,
    content_type='application/json'
)

minio_client.put_object(
    bucket_name,
    station_information_file,
    data=station_information_data,
    length=station_information_data.getbuffer().nbytes,
    content_type='application/json'
)

minio_client.put_object(
    bucket_name,
    meteo_file,
    data=meteo_data,
    length=meteo_data.getbuffer().nbytes,
    content_type='application/json'
)

print(f"Data successfully saved to MinIO in bucket '{bucket_name}' with timestamp '{timestamp}'")


