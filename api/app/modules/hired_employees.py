
import pandas as pd
from datetime import datetime, timedelta
from app.modules.ManageData import ManageData
from app.constants import Constants


class H_employees:

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


    def upload_hemp(self):
        print("acceced to upload HEMP")

        try:
            set_date = datetime.now()#event.get('date')
            today_date = datetime.strptime(set_date, "%Y-%m-%d")
            today_file_date = str(today_date.strftime('%Y-%m-%d %H-%M-%S'))
        except Exception:
            today_date = datetime.now() - timedelta(hours=5)
            today_file_date = str(today_date.strftime('%Y-%m-%d %H-%M-%S'))

        table_id = f"{Constants.DATASET}.{Constants.HI_EMP_TABLE}"
        path_filename = f'exception/{Constants.HI_EMP_TABLE}{Constants.OUTPUT_PATH.format(date=today_date)}{Constants.HI_EMP_TABLE}_{today_file_date}.csv'

        datos = pd.DataFrame([[self.id,
                                        self.name,
                                        self.datetime,
                                        self.department_id,
                                        self.job_id,
                                        ]],
                                      columns=list(Constants.fields_hemp.keys()))
        print(datos)
        datos.shape
        ManageData.upload_data(datos, table_id, path_filename, Constants.BUCKET_GCS, Constants.PROJECT)
        return 200
