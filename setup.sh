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
python flask-server/database.py
exec exit
exec $SHELL

