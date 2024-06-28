from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import requests
import polyline
import pandas as pd

def geocode(address):
    # Créer une instance de Nominatim pour le géocodage
    geolocator = Nominatim(user_agent="my-app")

    # Géocoder l'adresse
    location = geolocator.geocode(address)

    #print(location)

    # Extraire les coordonnées GPS
    latitude = location.latitude
    longitude = location.longitude

    exitValue = f"{latitude},{longitude}"

    # Retourner les coordonnées GPS
    return exitValue

def getRoute(start, end):
    
    start_point = geocode(start)
    end_point = geocode(end)

    # Send a request to the Directions API
    response = requests.get(f"https://maps.googleapis.com/maps/api/directions/json?origin={start_point}&destination={end_point}&mode=bicycling&key=AIzaSyDSRvzJ4FPYdcJfObeVIDPAGPLah_9izLY")

    #print((f"https://maps.googleapis.com/maps/api/directions/json?origin={start}&destination={end}&mode=driving&key=AIzaSyC5No11ZlEKIzaDO1PlpnjiG_YhMIOYZIs"))
    #print(response.json())

    # Extract the polyline from the response
    polyline_str = response.json()["routes"][0]["overview_polyline"]["points"]
    
    #global infos_direction
    #infos_direction = response.json()['routes'][0]['legs'][0]['steps']

    # Decode the polyline string
    points = polyline.decode(polyline_str)

    return points

def getVelibStationWithinXm(position, radius_m):

    # Récupérer les données des stations
    response = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json")
    data_station = response.json()
    
    response = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json")
    data = response.json()

    # Récupérer les données météo
    response = requests.get('https://api.meteo-concept.com/api/forecast/nextHours?token=beb09bde46829759e48f6158a0f8c1e08c3b1e62448e8529b6d162a4216849b3&insee=75056&hourly=true')
    data_meteo = response.json()

    # Créer une liste de dictionnaires avec les informations des stations
    stations = data['data']['stations']
    stations_list = []
    for station in stations:
        stations_list.append({
            'id': '1',
            'station_id': station['station_id'],
            'stationCode': station['stationCode'],
            'name': station['name'],
            'longitude': station['lon'],
            'latitude': station['lat'],
            'capacity': station['capacity']
        })

    station_status = data_station['data']['stations']
    bike_attributes = []
    for station_statu in station_status:
        if station_statu['station_id'] in [station['station_id'] for station in stations]:
            bike_attributes.append({
                'station_id': station_statu['station_id'],
                'ebike': station_statu['num_bikes_available_types'][1]['ebike'],
                'mechanical': station_statu['num_bikes_available_types'][0]['mechanical'],
                'num_bikes_available': station_statu['num_bikes_available'],
                'num_docks_available': station_statu['num_docks_available'],
                'is_installed': station_statu['is_installed'],
                'is_returning': station_statu['is_returning'],
                'is_renting': station_statu['is_renting'],
                'last_reported': station_statu['last_reported']
            })

    # Extraire les prévisions météo
    meteo_status = data_meteo['forecast'][0]
    
    meteo_attributes = [{
        'weather': meteo_status['weather'],
        'temp': meteo_status['temp2m'],
        'probarain': meteo_status['probarain'],
        'probafog': meteo_status['probafog'],
        'probawind70': meteo_status['probawind70'],
        'probawind100': meteo_status['probawind100'],
        'latitude': 48.8566,  # Exemple latitude pour les données météo (latitude de Paris)
        'longitude': 2.3522   # Exemple longitude pour les données météo (longitude de Paris)
    }]

    # Créer les DataFrames
    df = pd.DataFrame(stations_list)
    df_bike = pd.DataFrame(bike_attributes)
    df_meteo = pd.DataFrame(meteo_attributes)

    # Fusionner les DataFrames des stations et des statuts de vélos
    df = pd.merge(df, df_bike, on='station_id')

    # Coordonnées GPS à partir desquelles trouver les stations dans un rayon spécifié
    my_location = position

    # Filtrer les stations dans le rayon spécifié
    df['distance'] = df.apply(lambda row: geodesic(my_location, (row['latitude'], row['longitude'])).m, axis=1)
    stations_within_Xm = df[df['distance'] <= radius_m].copy()

    # Associer les données météo à chaque station (en supposant qu'il n'y a qu'un seul point de données météo pour Paris)
    for col in ['weather', 'temp', 'probarain', 'probafog', 'probawind70', 'probawind100']:
        stations_within_Xm.loc[:, col] = df_meteo[col].iloc[0]

    # Filtrer les stations dans un ordre croissant de distance
    stations_within_Xm = stations_within_Xm.sort_values(by=['distance'])

    # Réorganiser les colonnes dans l'ordre souhaité
    columns_order = ['id', 'station_id', 'stationCode', 'station_id', 'num_bikes_available', 'mechanical', 'ebike', 
                     'num_docks_available', 'is_installed', 'is_returning', 'is_renting', 'last_reported', 
                     'weather', 'temp', 'probarain', 'probafog', 'probawind70', 'probawind100']
    remaining_columns = [col for col in stations_within_Xm.columns if col not in columns_order]
    columns_order.extend(remaining_columns)
    stations_within_Xm = stations_within_Xm[columns_order]

    # Créer un tableau avec les données de stations_within_Xm
    stations_within_Xms = stations_within_Xm.to_dict('records')

    return stations_within_Xms

def getRoute2(start, end):

    start_point = geocode(start)
    end_point = geocode(end)

    # Send a request to the Directions API
    response = requests.get(f"https://maps.googleapis.com/maps/api/directions/json?origin={start_point}&destination={end_point}&mode=bicycling&key=AIzaSyDSRvzJ4FPYdcJfObeVIDPAGPLah_9izLY")

    infos_direction = response.json()['routes'][0]['legs'][0]['steps']

    infos_route = {
        'distance': [],
        'duration': [],
        'html_instructions': [],
        'length': []
    }
    for element in infos_direction:
        infos_route['distance'].append(element['distance']['text'])
        infos_route['duration'].append(element['duration']['text'])
        infos_route['html_instructions'].append(element['html_instructions'])

    infos_route['length'] = len(infos_route['distance'])

    return infos_route

#print(geocode("République"))
#print(getRoute2("République","Montparnasse"))
#print(getVelibStationWithinXm('48.8671878,2.3643917', 200))