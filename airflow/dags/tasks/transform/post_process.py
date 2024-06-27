import pandas as pd
from datetime import datetime

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


def convert_epoch_time_day_hour(epoch_time):
    return datetime.fromtimestamp(epoch_time).strftime('%A %H:%M')

def convert_epoch_time(epoch_time):
    return datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')