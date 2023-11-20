 
FROM python:3.7
 
WORKDIR /code
 
COPY ./requirements.txt /code/requirements.txt
 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
 
COPY ./app /code/app

# CMD exec gunicorn --bind :$PORT  --workers 2 --worker-class uvicorn.workers.UvicornWorker  --threads 6 app.main:app
CMD exec uvicorn app.main:app --host 0.0.0.0 --port 3000