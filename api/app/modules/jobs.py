import pandas as pd
from datetime import datetime, timedelta
from app.modules.ManageData import ManageData
from app.constants import Constants


class Jobs:

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
            

    def upload_jobs(self):
        print("acceced to upload dept")

        try:
            set_date = datetime.now()#event.get('date')
            today_date = datetime.strptime(set_date, "%Y-%m-%d")
            today_file_date = str(today_date.strftime('%Y-%m-%d %H-%M-%S'))
        except Exception:
            today_date = datetime.now() - timedelta(hours=5)
            today_file_date = str(today_date.strftime('%Y-%m-%d %H-%M-%S'))

        table_id = f"{Constants.DATASET}.{Constants.JOB_TABLE}"
        path_filename = f'exception/{Constants.JOB_TABLE}{Constants.OUTPUT_PATH.format(date=today_date)}{Constants.JOB_TABLE}_{today_file_date}.csv'
        print("select_Jobs")
        df_search = ManageData.get_data_from_bq(Constants.JOB_QUERY.format(id=self.id))
        print(df_search)
        if df_search.empty:
            datos = pd.DataFrame([[self.id,self.job]], columns=list(Constants.fields_job.keys()))
            print(datos)
            datos.shape
            ManageData.upload_data(datos, table_id, path_filename, Constants.BUCKET_GCS, Constants.PROJECT)
            return Constants.succesfully_m
        else:
            return Constants.id_exist