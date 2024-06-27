import json
import pandas as pd
from minio import Minio
import io
from collections import defaultdict
from airflow.decorators import task
from tasks.transform.post_process import data_prep


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
    metadata_bucket_name = "metadata"

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
            station_status_response = minio_client.get_object(
                bucket_name, station_status_path)
            data = json.loads(station_status_response.read())

            df = pd.json_normalize(data['data']['stations'])
            df_meteo = pd.json_normalize(data['weather'])
        else:
            continue

        station_information_path = f"{folder}/station_information.json"
        if station_information_path in files:
            station_information_response = minio_client.get_object(
                bucket_name, station_information_path)
            data_info = json.loads(station_information_response.read())
            df_informations = pd.json_normalize(data_info['data']['stations'])
        else:
            continue

        df['id'] = data['lastUpdatedOther']

        df = df[['id', 'stationCode', 'station_id', 'num_bikes_available', 'mechanical', 'ebike', 'num_docks_available', 'is_installed', 'is_returning',
                 'is_renting', 'last_reported', 'weather', 'temp', 'probarain', 'probafog', 'probawind70', 'probawind100', 'lon', 'lat', 'name']]

        df = df[df['stationCode'].notna()]

        if df['stationCode'].dtype == 'int64':
            df['stationCode'] = df['stationCode'].astype('int64')

        final_df = pd.concat([final_df, df], axis=0)

        # test
        final_df = data_prep(final_df)

        final_df['weekend'] = final_df['day_time'].str.contains(
            'Saturday|Sunday').astype(int)

        final_df['date_time'] = pd.to_datetime(
            final_df['date_time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        final_df['date'] = final_df['date_time'].dt.date
        final_df['date'] = pd.to_datetime(final_df['date'], format='%Y-%m-%d')
        final_df['time'] = final_df['date_time'].dt.time
        final_df['hour'] = final_df['date_time'].dt.hour
        final_df['minute'] = final_df['date_time'].dt.minute

        df_jours_ferie_response = minio_client.get_object(metadata_bucket_name, 'jours_feries_metropole.csv')
        df_jours_ferie = pd.read_csv(io.BytesIO(df_jours_ferie_response.read()))

        df_jours_ferie['date'] = pd.to_datetime(
            df_jours_ferie['date'], format='%Y-%m-%d')

        final_df = pd.merge(final_df, df_jours_ferie, how='left', on='date')
        final_df.drop(['annee', 'zone'], axis=1, inplace=True)
        final_df['jour_ferie'] = final_df['nom_jour_ferie'].notna().astype(int)
        final_df.drop(['nom_jour_ferie'], axis=1, inplace=True)

        station_info_response = minio_client.get_object(metadata_bucket_name, 'station_information.json')
        station_info_data = json.loads(station_info_response.read())
        df_station_info = pd.json_normalize(station_info_data['data'], "stations")

        df_station_info = pd.json_normalize(data['data'], "stations")
        df_station_info['stationCode'] = df_station_info['stationCode'].astype(
            str)
        df_station_info = df_station_info[[
            'stationCode', 'name', 'capacity', 'lat', 'lon']]
        final_df = pd.merge(final_df, df_station_info,
                            how='left', on='stationCode')

        futur_epochs = [(5, 300), (10, 600), (15, 900),
                        (20, 1200), (25, 1500), (30, 1800)]
        dfs_velib = [final_df] * len(futur_epochs)

        availability_dict = final_df.set_index(['stationCode', 'id']).groupby(level=[0, 1])[
            'num_docks_available'].first().to_dict()
        sorted_epochs = sorted(set(final_df['id']))

        for i in range(len(futur_epochs)):
            df_velib_fut = dfs_velib[i].copy()
            # on rajoute une feature qui indique le moment dans le futur pour lequel on veut prédire les disponibilités
            df_velib_fut['futur_min'] = futur_epochs[i][0]
            # On rajoute 5 minutes aux timestamps (= 300 epochs)
            df_velib_fut['futur_epoch'] = df_velib_fut['id'] + \
                futur_epochs[i][1]

            df_tuples = list(
                zip(df_velib_fut['stationCode'].tolist(), df_velib_fut['futur_epoch'].tolist()))

            futur_availability = []

            for tuple in df_tuples:
                avail = availability_dict.get(tuple)
                if avail == None:
                    later_timestamps = [
                        timestamp for timestamp in sorted_epochs if timestamp >= tuple[1]]
                    # if there exists availability data after the current epoch + 5 min, we take the next timestamp we can find
                    # else, avail keeps the value None
                    if len(later_timestamps) != 0:
                        avail = availability_dict.get(
                            (tuple[0], later_timestamps[0]))
                futur_availability.append(avail)

            df_velib_fut['futur_availability'] = futur_availability

            dfs_velib[i] = df_velib_fut

        df_velib_futur = pd.concat(dfs_velib, axis=0)
        df_cluster_station_response = minio_client.get_object(metadata_bucket_name, 'station_cluster.csv')
        df_cluster_station = pd.read_csv(io.BytesIO(df_cluster_station_response.read()))


        df_cluster_station['stationCode'] = df_cluster_station['stationCode'].astype(
            str)
        df_velib_futur = pd.merge(
            df_velib_futur, df_cluster_station, how='left', on='stationCode')

        df_velib_futur = df_velib_futur[~df_velib_futur['Cluster'].isna()]
        df_velib_training = df_velib_futur[
            ['stationCode', 'Cluster', 'num_docks_available', 'capacity', 'hour', 'minute', 'temp', 'probarain', 'weekend',
             'jour_ferie', 'lat', 'lon', 'futur_min', 'futur_availability']]
        
        final_df = df_velib_training


    print(final_df.columns)
    print(len(final_df))
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
