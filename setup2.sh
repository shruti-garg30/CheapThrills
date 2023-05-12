#!/bin/bash
pip install flask
pip install amadeus
pip install flask_sqlalchemy
pip install flask_cors
pip install pyspark
pip install psycopg2
pip install flask_pymongo
cd client
npm install 
cd ..
docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=123 -d postgres
docker exec -u postgres postgres createdb cheapThrills
docker pull mongo
docker run --name mongo -p 27017:27017 -d mongo
python flask-server/database.py
exec exit
exec $SHELL
