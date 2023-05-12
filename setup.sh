#!/bin/bash
pip install flask
pip install amadeus
pip install flask_sqlalchemy
pip install flask_cors
pip install pyspark
pip install psycopg2
pip install flask_pymongo
npm install 
docker run --name postgres -p 5432:5432 --restart always -e POSTGRES_PASSWORD=123 -d postgres
docker exec -it postgres psql -U postgres -c "CREATE DATABASE cheapThrills;"
docker run --name mongo -e MONGO_INITDB_ROOT_USERNAME=mongo -e MONGO_INITDB_ROOT_PASSWORD=123 -p 27017:27017 -d mongo
docker exec -it mongo mongo admin --eval "db.createUser({user: 'mongo', pwd: '123', roles: [{role: 'root', db: 'admin'}]}); db.createDatabase('cheapThrills');"
python /flask-server/database.py
exec exit
exec $SHELL

