from fastapi import FastAPI
from app.modules.hired_employees import H_employees
from app.modules.departments import Departments
from app.modules.jobs import Jobs
from app.DataModel.datamodel import DepartmentsM, Hired_employeesM, JobsM
import os

app = FastAPI()


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./app/CREDENTIALS.json"

@app.get("/")
def read_root():
    return {"Welcome to Challenge"}


@app.post("/challenge/departments/", status_code=200)
def upload_depts(departments: DepartmentsM):
    id_status_code = "INCORRECT DATA"  # 400
    status_code = ""
    try:
        parameter_dict = dict(
            id=departments.id,
            department=departments.department
                         )
        
    except Exception as e:
        error_detail = f"Error Message: {e}"
        print(error_detail)
        status_code = id_status_code
        return status_code

    data = Departments(**parameter_dict).upload_dept()
    return data


@app.post("/challenge/hired_employees/", status_code=200)
def upload_hr_emp(h_employees: Hired_employeesM):
    id_status_code = "INCORRECT DATA"  # 400
    status_code = ""
    try:
        parameter_dict = dict(
            id=h_employees.id,
            name=h_employees.name,
            datetime=h_employees.datetime,
            department_id=h_employees.department_id,
            job_id=h_employees.job_id
                         )
        
    except Exception as e:
        error_detail = f"Error Message: {e}"
        print(error_detail)
        status_code = id_status_code
        return status_code

    data = H_employees(**parameter_dict).upload_hemp()
    print(data)
    return data


@app.post("/challenge/jobs/", status_code=200)
def upload_jobs(jobs: JobsM):
    id_status_code = "INCORRECT DATA"  # 400
    status_code = ""
    try:
        parameter_dict = dict(
            id=jobs.id,
            job=jobs.job
                         )
        
    except Exception as e:
        error_detail = f"Error Message: {e}"
        print(error_detail)
        status_code = id_status_code
        return status_code

    data = Jobs(**parameter_dict).upload_jobs()
    return data
