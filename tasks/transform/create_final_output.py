import os 
import json
import pandas as pd

final_df = pd.DataFrame()

for folder in os.listdir("data"):
    if folder == ".DS_Store":
        continue
    else:
        path = os.path.join("data", folder, "station_status.json")
    
    # Open the json file
    with open(path) as f:
        data = json.load(f)

    # Create a pandas dataframe from the json file
    df = pd.json_normalize(data['data']['stations'])
    df_meteo = pd.json_normalize(data['weather'])

    path_info = os.path.join("data", folder, "station_information.json")
    with open(path_info) as f:
        data_info = json.load(f)
    df_informations = pd.json_normalize(data_info['data']['stations'])

    # Clean the weather value:

    cleanup = {'weather': {'Ensoleillé': 0, 
                        'Nuageux': 1,
                        'Peu nuageux': 1,
                        'Très nuageux': 1,
                        'Pluie': 2,
                        'Averses': 2,
                        'Neige': 3, 
                        'Brouillard': 4, 
                        'Orage': 5,
                        'Couvert' : 6,
                        'Ciel voilé': 7}}

    df_meteo.replace(cleanup, inplace=True)

    # Add the id (timestamp) to the dataframe and change the order of the columns
    df['id'] = data['lastUpdatedOther']

    # Add the type of bike available
    print(folder)
    df['mechanical'] = df['num_bikes_available_types'].apply(lambda x: x[0]['mechanical'])
    df['ebike'] = df['num_bikes_available_types'].apply(lambda x: x[1]['ebike'])
    df.drop(['num_bikes_available_types'], axis=1, inplace=True)


    # Add the weather data to the dataframe
    df = pd.merge(df, df_meteo, how='cross')

    # Ajouter la colonne 'lon', 'lat' et 'name' de df_informations dans df en se basant sur le station_id
    df = pd.merge(df, df_informations[['station_id', 'lon', 'lat', 'name']], on='station_id', how='left')

    # Drop useless columns
    df.drop(['numBikesAvailable','numDocksAvailable'], axis=1, inplace=True)

    df = df[['id','stationCode','station_id','num_bikes_available','mechanical','ebike','num_docks_available','is_installed','is_returning','is_renting','last_reported','weather','temp','probarain','probafog','probawind70','probawind100', 'lon', 'lat', 'name']]

    # Delete NoneType values in the dataframe on the column 'stationCode'
    df = df[df['stationCode'].notna()]

    # Problem with a type of station with "relai" in the stationCode column
    if df['stationCode'].dtype == 'int64':
        df['stationCode'] = df['stationCode'].astype('int64')
    else:
        pass

    final_df = pd.concat([final_df, df], axis=0)

print(final_df)

# extract the dataframe to a csv file
final_df.to_csv('output/250624-bis.csv', index=False)