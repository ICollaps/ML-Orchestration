import requests
import json
import os
import datetime
import math
import time
import pandas as pd
from velibData import getVelibStationStatus, getVelibStationInformations
from meteo import getMeteo, convertMeteoValue
import tqdm

for i in range(0, 10):
    # get the actual date in timestamp
    now = datetime.datetime.now()
    timestamp = math.trunc(datetime.datetime.timestamp(now))

    data_velib_status = getVelibStationStatus()
    data_velib_informations = getVelibStationInformations()
    data_meteo = getMeteo()

    data_velib_status['weather'] = {'weather':convertMeteoValue(data_meteo),
                                    'temp':data_meteo['forecast'][0]['temp2m'],
                                    'probarain': data_meteo['forecast'][0]['probarain'],
                                    'probafog': data_meteo['forecast'][0]['probafog'],
                                    'probawind70': data_meteo['forecast'][0]['probawind70'],
                                    'probawind100': data_meteo['forecast'][0]['probawind100'],}

    # Create a folder on the data folder of the project named "data"
    if not os.path.exists("data/" + str(timestamp)):
        os.makedirs("data/" + str(timestamp))

    # Create a file from the response data and save it in the folder created above
    with open("data/" + str(timestamp) + "/station_status.json", "w") as f:
        f.write(json.dumps(data_velib_status))

    with open("data/" + str(timestamp) + "/station_information.json", "w") as f:
        f.write(json.dumps(data_velib_informations))

    with open("data/" + str(timestamp) + "/meteo.json", "w") as f:
        f.write(json.dumps(data_meteo))

    # Wait 5 minutes
    time.sleep(300)