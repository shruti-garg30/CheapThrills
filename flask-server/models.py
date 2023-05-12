from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from flask_cors import CORS
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from flask_pymongo import PyMongo

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:123@localhost:5432/cheapThrills'

app.config['MONGO_URI'] = 'mongodb://localhost:27017/cheapThrills'
mongo = PyMongo(app)

db = SQLAlchemy(app)
CORS(app)

conf = pyspark.SparkConf().set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")\
        .setMaster("local")\
        .setAll([("spark.driver.memory", "40g"),("spark.executive.memory", "50g")])
sc = SparkContext.getOrCreate(conf)
spark_db = SparkSession(sc)

def app_context():
    with app.app_context():
        yield

app.app_context().push()

class Cities(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=False)
    city_name = db.Column(db.String(), nullable=False)
    city_latitude = db.Column(db.Float)
    city_longitude = db.Column(db.Float)
    airport_code = db.Column(db.String())
    average_cost = db.Column(db.Float, nullable=False)

    def __repr__(self):
        return f"City: \t Name: {self.city_name} \t Code: {self.airport_code} \t Latitude: {self.city_latitude}"
    
    def __init__(self, city_id, city_name, city_latitude, city_longitude, airport_code, average_cost):
        self.id = city_id
        self.city_name = city_name
        self.city_latitude = city_latitude
        self.city_longitude = city_longitude
        self.airport_code = airport_code
        self.average_cost = average_cost
              
class Place_of_Interest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    city_id = db.Column(db.Integer, nullable=True)
    place_name = db.Column(db.String(), nullable=False)

    def __repr__(self):
        return f"Place of Interest: \t Place: {self.place_name} \n"
    
    def __init__(self, city_id, place_name):
        self.city_id = city_id
        self.place_name = place_name

class Person(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(200), nullable=False)
    email = db.Column(db.String(300), nullable=False, primary_key=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"Person: \n Name: {self.name}, \t Email: {self.email}"
    
    def __init__(self, name, email):
        self.name = name
        self.email = email