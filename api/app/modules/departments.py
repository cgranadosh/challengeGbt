import pandas as pd
from datetime import datetime, timedelta
from app.modules.ManageData import ManageData
from app.constants import Constants




class Departments:

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        def upload_dept(self):
            print("acceced to upload dept")

            try:
                set_date = datetime.now()#event.get('date')
                today_date = datetime.strptime(set_date, "%Y-%m-%d")
                today_file_date = str(today_date.strftime('%Y-%m-%d %H-%M-%S'))
            except Exception:
                today_date = datetime.now() - timedelta(hours=5)
                today_file_date = str(today_date.strftime('%Y-%m-%d %H-%M-%S'))

            table_id = f"{Constants.DATASET}.{Constants.DEPT_TABLE}"
            path_filename = f'exception/{Constants.DEPT_TABLE}{Constants.OUTPUT_PATH.format(date=today_date)}{Constants.DEPT_TABLE}_{today_file_date}.csv'
            print("select_departments")
            df_search = ManageData.get_data_from_bq(Constants.DEPT_QUERY.format(id=self.id))
            print(df_search)
            if df_search.empty:
                 datos = pd.DataFrame([[self.id,self.department]], columns=list(Constants.fields_dept.keys()))
                 print(datos)
                 datos.shape
                 ManageData.upload_data(datos, table_id, path_filename, Constants.BUCKET_GCS, Constants.PROJECT)
                 return 200
            else:
                 return Constants.id_exist 
            