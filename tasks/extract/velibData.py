import requests
import datetime

def epoch_to_date(timestamp):
    # Convertir le timestamp en objet datetime
    dt_object = datetime.datetime.fromtimestamp(timestamp)

    # Formater la date et l'heure au format lisible par l'homme
    formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")

    # Retourner la date et l'heure formatées
    return formatted_date


def getVelibStationStatus():
        
    # Get the data from the API
    response = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json")
    data = response.json()

    return data

def getVelibStationInformations():

    response2 = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json")
    data2 = response2.json()

    return data2

