# challengeGbt

Primero realizar las cargas de los datos con el archivo Upload_history.ipynb
Se deben cambiar los valores de la segunda celda del script, dependiendo del proyecto con el que se vaya a ejecutar y el storage donde se pongan los archivos, crear el dataset en BQ llamado: challengeglobant


API
Prerequisitos:
Tener instalado docker o tener un servidor con docker
Haber cargado el historico, puesto que es importante que esten creadas las tablas, en BQ.

Ejecución:
1. Construir la imagen docker ejecutando el siguiente comando en el CMD: 
         docker build -t apptest .
2. Para ejecutar la imagen en un container ejecutar: 
         docker run -dp 3000:3000 --name testcont apptest
4. Acceder al localhost:3000
5. primera pantalla es solo un saludo ["Welcome to Challenge"]
6. Acceder a localhost:3000/Docs donde aparecera la información de los post que se le pueden hacer a la API los cuales son: 
   Departments:
        {
            "id": 0,
            "department": "string"
        }
    Hired_employees: 
    {
            "id": 0,
            "name": "string",
            "datetime": "string",
            "department_id": 0,
            "job_id": 0
        }
    Jobs: 
    {
        "id": 0,
        "job": "string"
        }

SE DEBEN AGREGAR CREDENCIALES DE ACCESO, DADO QUE ESTAS NO SE PROPORCIONAN POR SEGURIDAD DE MI ESPACIO DE TRABAJO.
para lo cual se deben generar unas credenciales en GCP en formato json, ubicarlas en el mismo path del archivo main con el nombre CREDENTIALS.json


POr ultimo se elaboró un panel el cual se puede ver en el siguiente link: 
https://lookerstudio.google.com/reporting/1c1b3c09-81b5-444b-ac0e-cc9da23d6bdd
