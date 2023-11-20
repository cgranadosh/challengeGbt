import os

env = os.environ.get("ENVIRONMENT")

class Constants:

    space = "%20"
    hour = space + "00:00:00"
    userid = 119
    ENV = env  # os.environ.get("environment")
    PROJECT = "cameraanalytics"  # os.environ.get("project")
    key = "d9286c5881aa4be3b6152098937846b7"
    base_url = "https://cloud.seemetrix.3divi.com/analytics/api/"
    OUTPUT_PATH = "year={date.year}/month={date.month:02}/day={date.day:02}"
    BUCKET_GCS = f"{PROJECT}_data"
    DATASETFINAL = f"{PROJECT}_{ENV}_final"
    PATH_EXCEPT = f"gs://{BUCKET_GCS}/except"
    TABLE_RAW = f"{DATASETFINAL}.raw_data"
    TABLE_DEVICES_ON = f"{DATASETFINAL}.device_data_on"
    TABLE_DEVICES_OFF = f"{DATASETFINAL}.device_data_off"
    TABLE_DEVICES_FROZEN = f"{DATASETFINAL}.device_data_fro"
    PROJECT_ID = "wi-fi-analytics-357217"
    FILE_RAW = f"raw_{ENV}_file"
    FILE_DEVICES_ON = "devices_file_on"
    FILE_DEVICES_OFF = "devices_file_off"
    FILE_DEVICES_FROZEN = "devices_file_fro"
    query_delete = [f"delete from {TABLE_DEVICES_ON} where 1=1",
                    f"delete from {TABLE_DEVICES_OFF} where 1=1",
                    f"delete from {TABLE_DEVICES_FROZEN} where 1=1"
                    ]
    delete_query = f"DELETE FROM `cameraanalytics_{ENV}_final.raw_data` \
    WHERE PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', (split(start_date, '.'))[safe_ordinal(1)]) >= '{{date}}'"

    query_string = f"SELECT date_add(max(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',(split(start_date, '.'))[safe_ordinal(1)])), INTERVAL -12 HOUR)  MAX_DATE from `wi-fi-analytics-357217.cameraanalytics_{ENV}_final.raw_data`"
