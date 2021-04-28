# Voucher-API

### Requirement
* Docker Installed on system

### Libraries
Pandas\
Psycopg2\
FastAPI\
PyArrow\
Pydantic\
Uvicorn

### How to run
1) Download the repo on your system or clone it using below command if git is installed\
    git clone https://github.com/taimurKhann/Voucher-API.git
2) Move into Voucher-API folder\
    cd Voucher-API
3) Run below command to run the docker containers\
    docker-compose up
4) Once containers are up and running, open another terminal and run below command\
    docker exec -it app /bin/bash
5) Once connected to app conatiner, run below command\
    python /Voucher-API/main.py
6) After script completed, API is ready to be use
7) Go to browser and type 127.0.0.1:8000/docs
8) It will open FastAPI built in SwaggerUI to test Api request
    
