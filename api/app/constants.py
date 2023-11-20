
class Constants:
    
    ENV = 'dev'
    PROJECT = " praxis-road-356512"
    BUCKET_GCS = f"challengeglobant"
    INPUT_PATH_RAW = f"gs://{BUCKET_GCS}/raw"
    PATH_FEATURES = f"gs://{BUCKET_GCS}/features"
    PATH_EXCEPT = f"gs://{BUCKET_GCS}/except"
    DATASET = 'challengeglobant'
    DEPT_TABLE = "departments"
    HI_EMP_TABLE = 'hired_employees'
    JOB_TABLE = 'jobs'    
    OUTPUT_PATH="/year={date.year}/month={date.month:02d}/day={date.day:02d}/"
    fields_hemp = {"id":"","name":"","datetime":"","department_id":"","job_id":""}
    fields_dept = {"id":"","department":""}
    fields_job = {"id":"","job":""}
