from google.cloud import storage, bigquery


class ManageData:

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
            # get client
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
            raise f"error loading information to bq. please check the process manually: {e}"
        return
    
    @staticmethod
    def upload_storage(bucket_gcs,df, path_filename):
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_gcs)
            bucket.blob(path_filename, chunk_size=262144).upload_from_string(df.to_csv(), 'text/csv')
        except Exception as e:
            print(f"Error loading Dataframe to Storage, please check: {e}")
        
        return
    

    @staticmethod
    def upload_data(df, table, path_storage, bucket, project):
        
        try:
            ManageData.to_bq(df, table, project)
        except:
            ManageData.upload_storage(bucket, df, path_storage) 
        return
