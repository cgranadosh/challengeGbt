from pydantic import BaseModel


class DepartmentsM(BaseModel):
    id: int
    department: str

class Hired_employeesM(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

class JobsM(BaseModel):
    id: int
    job: str
