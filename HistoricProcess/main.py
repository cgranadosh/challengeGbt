import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from constants import Constants
from ManageData import ManageData
import os

userid = Constants.userid
key = Constants.key
base_url = Constants.base_url
list_names = {'a': 'Edad', 'at': 'watcher', 'b': 'start_date',
              'd': 'device_id', 'e': 'end_time', 'em': 'emotion', 'i': 'total_time',
              'o': 'session_id', 's': 'genero', 'v': 'video'
              }
sele_cols = ['Edad', 'watcher', 'start_date', 'device_id', 'end_time',
             'emotion', 'total_time', 'session_id', 'genero']


def get_data_devices_off(date_usage):
    complement = f"user/{userid}/devices/?key={key}"
    api_url = base_url + complement
    url = api_url
    print(url)
    response = requests.get(url)
    response.status_code
    list_devices = response.json().get("offline")
    dfitems = pd.DataFrame.from_records(list_devices)

    table_final = Constants.TABLE_DEVICES_OFF
    project_id = Constants.PROJECT_ID
    file_prefix = Constants.FILE_DEVICES_OFF
    try:
        data_bq(dfitems, table_final, project_id, date_usage, file_prefix)
    except:
        print("No fue posible agregar la data dispositivos on")
    return dfitems


def get_data_devices_on(date_usage):
    complement = f"user/{userid}/devices/?key={key}"
    api_url = base_url + complement
    url = api_url
    print(url)
    response = requests.get(url)
    response.status_code
    list_devices = response.json().get("online")
    dfitems = pd.DataFrame.from_records(list_devices)

    table_final = Constants.TABLE_DEVICES_ON
    project_id = Constants.PROJECT_ID
    file_prefix = Constants.FILE_DEVICES_ON
    try:
        data_bq(dfitems, table_final, project_id, date_usage, file_prefix)
    except:
        print("No fue posible agregar la data 2")
    return dfitems


def get_data_devices_frozen(date_usage):
    complement = f"user/{userid}/devices/?key={key}"
    api_url = base_url + complement
    url = api_url
    print(url)
    response = requests.get(url)
    response.status_code
    list_devices = response.json().get("frozen")
    dfitems = pd.DataFrame.from_records(list_devices)

    table_final = Constants.TABLE_DEVICES_FROZEN
    project_id = Constants.PROJECT_ID
    file_prefix = Constants.FILE_DEVICES_FROZEN
    try:
        data_bq(dfitems, table_final, project_id, date_usage, file_prefix)
    except:
        print("No fue posible agregar la data 2")
    return dfitems


def get_raw_data(star_date, end_date, date_usage):
    b = star_date
    e = end_date
    tzo = "0"
    complement = f"statistics/user/{userid}/raw/data/?key={key}&tzo={tzo}&b={b}&e={e}"
    api_url = base_url + complement
    url = api_url
    print(url)
    response_raw = ""
    try:
        response_raw = requests.get(url)
    except:
        print("API no responde")
        return response_raw.status_code
    df_data = pd.DataFrame()
    try:
        json_raw = response_raw.json()
        list_data = json_raw.get("objects")
        df_raw = pd.DataFrame.from_records(list_data)
        df_raw.rename(columns=list_names, inplace=True)
        df_data = df_raw[sele_cols]
        table_final = Constants.TABLE_RAW
        project_id = Constants.PROJECT_ID
        file_prefix = Constants.FILE_RAW
        print(table_final, project_id, file_prefix)
        data_bq(df_data, table_final, project_id, date_usage, file_prefix)
    except:
        print("There is not data")

    return df_data


def data_bq(df, table_final, project_id, date_transform, file_prefix):
    try:
        print("access to try")
        ManageData.to_bq(df=df,
                         table_id=table_final,
                         project=project_id
                         )
    except:
        print("access to except")
        if df["watcher"]:
            df = df.astype({'watcher': 'string',
                            'emotion': 'string'})

        str_date = str(date_transform.strftime('%Y%m%d%H:%M:%S.%s'))
        path_date = Constants.PATH_EXCEPT + Constants.OUTPUT_PATH.format(date=date_transform)
        print(path_date)
        file_name = path_date + file_prefix + str_date + '.parquet'
        # upload to Storage
        df.to_parquet(file_name)


def main(event, context):
    set_date = datetime.now()  # event.get('date')
    set_date_hour = set_date + timedelta(hours=4)
    print(set_date)
    try:
        set_date_u = set_date - timedelta(days=1)
        today_date = datetime.strptime(str(set_date).split(" ")[0], "%Y-%m-%d")
        str_today_datetime = str(datetime.strptime(str(set_date).split(".")[0], "%Y-%m-%d %H:%M:%S"))
        today_file_date = str(today_date.strftime('%Y-%m-%d'))
    except Exception:
        today_date = datetime.now() - timedelta(days=1)
        today_file_date = str(today_date.strftime('%Y-%m-%d'))

    end_date = set_date_hour.strftime("%Y/%m/%d") + Constants.space + set_date_hour.strftime("%H:%M:%S")
    start_date = set_date_u.strftime("%Y/%m/%d") + Constants.hour
    # start_date = '2022/08/01%2000:00:00'

    for delete in Constants.query_delete:
        try:
            ManageData.delete_tables(Constants.PROJECT_ID, delete)
        except:
            print("No se ejecuto: ", delete)

    get_data_devices_off(set_date)
    # print(devices_offline)
    get_data_devices_on(set_date)
    try:
        get_data_devices_frozen(set_date)
    except:
        print("no fue posible cargar frozen")

    # get max data
    max_date_result = ManageData.get_data_table_bigquery(Constants.query_string)
    str_date_value = str(max_date_result.MAX_DATE.item()).split("+")[0]
    str_date, str_hour = str_date_value.split(" ")

    # delete_data with max
    q_delete_table = Constants.delete_query.format(date=str_date_value)
    ManageData.delete_tables(Constants.PROJECT_ID, q_delete_table)
    # print(devices_online)
    # get data
    start_date_concat = str_date + Constants.space + str_hour
    print(start_date_concat.replace("-", "/"), "start date")
    print(end_date, "end date")
    raw_data_day = get_raw_data(start_date_concat.replace("-", "/"), end_date, set_date)
    # print(raw_data_day)
