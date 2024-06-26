from airflow.decorators import task
import clickhouse_connect
import pandas as pd
from minio import Minio
from io import StringIO

@task
def load(timestamp: int):

    client = clickhouse_connect.get_client(host='clickhouse')
    print("Connecting to clickhouse")

    create_table_query = '''
        CREATE TABLE IF NOT EXISTS velib (
            id UInt64,
            stationCode UInt64,
            station_id UInt64,
            num_bikes_available UInt64,
            mechanical UInt64,
            ebike UInt64,
            num_docks_available UInt64,
            is_installed UInt64,
            is_returning UInt64,
            is_renting UInt64,
            last_reported UInt64,
            weather UInt64,
            temp Int64,
            probarain Int64,
            probafog Int64,
            probawind70 Int64,
            probawind100 Int64,
            lon Float64,
            lat Float64,
            name String
        ) ENGINE = MergeTree()
        ORDER BY id;
    '''

    client.command(create_table_query)
    print("table created or already existing")

    # Connect to MinIO
    minio_url = "minio:9000"
    minio_client = Minio(
        minio_url,
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    print("Connecting to MinIO")

    bucket_name = "output"
    objects = minio_client.list_objects(bucket_name, recursive=True)

    for obj in objects:
        if obj.object_name == f"{timestamp}.csv":
            print(f"Processing {obj.object_name}")
            
            response = minio_client.get_object(bucket_name, obj.object_name)
            csv_data = response.read().decode('utf-8')
            response.close()
            response.release_conn()

            df = pd.read_csv(StringIO(csv_data), delimiter=',')
            print(df.columns)

            insert_query = '''
                INSERT INTO velib (
                    id, stationCode, station_id, num_bikes_available, mechanical, ebike,
                    num_docks_available, is_installed, is_returning, is_renting, last_reported,
                    weather, temp, probarain, probafog, probawind70, probawind100, lon, lat, name
                ) VALUES
            '''
            values = df.to_records(index=False).tolist()
            client.insert('velib', values)
            print(f"Inserted data from {obj.object_name} into ClickHouse")

    

    