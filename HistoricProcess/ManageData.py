from google.cloud import storage, bigquery
import pandas as pd
from datetime import datetime


class ManageData:

    def get_data_table_bigquery(query_string):

        bqclient = bigquery.Client()

        # Download query results.

        dataframe = (
            bqclient.query(query_string)
            .result()
            .to_dataframe(
                # Optionally, explicitly request to use the BigQuery Storage API. As of
                # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
                # API is used by default.
                create_bqstorage_client=True,
            )
        )
        return dataframe

    @staticmethod
    def to_bq(df, table_id, project, today_file_date=None, schema=None):
        """
        df = dataframe to upload
        table_id = dataset + table name
        project = project to upload data
        today_file_date = optional var to add to dataframe that upload
        schema = optional var to define column types and names
        """

        try:
            today_date = datetime.today()  # '2022-01-06'
            today_file_date = str(today_date.strftime('%Y-%m-%d'))
        except Exception as e:
            print(e)
            today_date = datetime.today()  # -timedelta(hours=5)
            today_file_date = str(today_date.strftime('%Y-%m-%d'))

        try:
            # get client
            print("Carga Datos BQ")
            bq_client = bigquery.Client(project=project)

            if schema is None:
                # save to BQ
                job = bq_client.load_table_from_dataframe(df, table_id)
                # Wait for the job to complete.
                job.result()
            else:
                job = bq_client.load_table_from_dataframe(df, table_id, job_config=schema)
                job.result()

        except Exception as e:
            return f"error loading information to bq. please check the process manually: {e}"
        return

    @staticmethod
    def delete_tables(project_id, query_delete):
        bigquery_client = bigquery.Client(project=project_id)
        print(query_delete)
        job = bigquery_client.query(query_delete)
        job.result()
        print("Deleted!")

    @staticmethod
    def upload_storage(bucket_gcs, df, path_filename):
        try:
            client = storage.Client()
            bucket = storage_client.get_bucket(bucket_gcs)
            bucket.blob(path_filename, chunk_size=262144).upload_from_string(df.to_csv(), 'text/csv')
        except Exception as e:
            print(f"Error loading Dataframe to Storage, please check: {e}")
        return
