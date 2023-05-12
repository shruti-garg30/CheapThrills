from flask import request
from datetime import date, datetime
from hashlib import new

from flask_cors import CORS
from sqlalchemy import inspect

import math
from amadeus import Client, ResponseError
import json
from json.encoder import INFINITY
import random
from flask_pymongo import PyMongo
from pymongo import MongoClient
from bson import json_util
from models import app, db, Cities, Place_of_Interest, Person, sc, spark_db
from pyspark.sql.functions import split, col
import pandas as pd

app.config['MONGO_URI'] = 'mongodb://localhost:27017/cheapThrills'
mongo = PyMongo(app)
recommendations = spark_db.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/cheapThrills.cheapThrills.recommendations").load()
print(recommendations.show(2))

flights = spark_db.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/cheapThrills.cheapThrills.flights").load()
# recommendations.createOrReplaceTempView("recommendations")
db_mongo = mongo.db['cheapThrills']
recommendations_mongo = db_mongo['recommendations']
flights_mongo = db_mongo['flights']

def format_city(city, poi_list, actual_budget):
    return {
        "name": city.city_name,
        "airport_code": city.airport_code,
        "city_id": city.id,
        "latitude": city.city_latitude,
        "longitude": city.city_longitude,
        "poi": poi_list,
        "budget": actual_budget
    }

def format_user(person):
    return {
        "name": person.name,
        "email": person.email

    }

class Flight():
    def __init__(self, src, dest, price, date):
        self.src = src
        self.dest = dest
        self.price = price
        self.date = date
        self.timestamp = datetime.now()

def format_flight(flight):
    return {
        "source": flight.src,
        "destination": flight.dest,
        "price": flight.price,
        "date": flight.date,
        "timestamp": flight.timestamp

    }

def format_city_poi(name, airport_code, city_id, latitude, longitude, poi, budget):
    return {
        "name": name,
        "airport_code": airport_code,
        "city_id": city_id,
        "latitude": latitude,
        "longitude": longitude,
        "poi": poi,
        "budget": budget
        
    }

def get_square_bounds(lat, lon, distance):
    R = 6371.01 # Earth's radius in km
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    dist_rad = distance / R

    # Calculate maximum and minimum latitude
    max_lat = math.asin(math.sin(lat_rad) * math.cos(dist_rad) + math.cos(lat_rad) * math.sin(dist_rad) * math.cos(0))
    min_lat = math.asin(math.sin(lat_rad) * math.cos(dist_rad) + math.cos(lat_rad) * math.sin(dist_rad) * math.cos(math.pi))

    # Calculate maximum and minimum longitude
    max_lon = lon_rad + math.atan2(math.sin(math.pi/2) * math.sin(dist_rad) * math.cos(lat_rad), math.cos(dist_rad) - math.sin(lat_rad) * math.sin(max_lat))
    min_lon = lon_rad + math.atan2(math.sin(-math.pi/2) * math.sin(dist_rad) * math.cos(lat_rad), math.cos(dist_rad) - math.sin(lat_rad) * math.sin(min_lat))

    # Convert back to degrees
    max_lat = math.degrees(max_lat)
    min_lat = math.degrees(min_lat)
    max_lon = math.degrees(max_lon)
    min_lon = math.degrees(min_lon)

    return (max_lat, min_lat, max_lon, min_lon)

class Preferences():
    def __init__(self, budget, start_date, end_date, origin_city):
        self.budget = budget
        self.start_date = start_date
        self.end_date = end_date
        self.origin_city = origin_city

def format_preference(preference):
    return {
        "budget": preference.budget,
        "originCity": preference.origin_city,
        "startDate": preference.start_date,
        "endDate": preference.end_date
    }

def findFlightPrices(src, dest, date):
    amadeus = Client()

    result = []
    try:
        response = amadeus.shopping.flight_offers_search.get(
            originLocationCode=src, destinationLocationCode=dest, departureDate=date, adults=1)
        result = response.data

        price = INFINITY
        for j in result:
            price = min(price, float(j['price']['total']))
        
        if price == INFINITY:
            return 0
        else:
            return price

    
    except ResponseError as error:
        print(error)
    
def calculateNumDays(preference):
    start_date = preference.start_date
    end_date = preference.end_date
    diff = date(int(end_date.split('-')[0]), int(end_date.split('-')[1]), int(end_date.split('-')[2])) -  date(int(start_date.split('-')[0]), int(start_date.split('-')[1]), int(start_date.split('-')[2]))
    num_days = diff.days
    return num_days

def addFlightObjectToDatabase(src, dest, price_to, price_from, start_date, end_date):
    flight_obj = Flight(src, dest, price_to, start_date)
    # df = spark_db.createDataFrame(format_flight(flight_obj))
    # df.write.format('com.mongodb.spark.sql.DefaultSource').option( "uri", "mongodb://localhost:27017/cheapThrills.cheapThrills.flights").save()
    flights_mongo.insert_one(format_flight(flight_obj))

    flight_obj = Flight(dest, src, price_from, end_date)
    # df = spark_db.createDataFrame(format_flight(flight_obj))
    # df.write.format('com.mongodb.spark.sql.DefaultSource').option( "uri", "mongodb://localhost:27017/cheapThrills.cheapThrills.flights").save()
    flights_mongo.insert_one(format_flight(flight_obj))

def createTargetCities(city_list, curr_city, preference, num_days):
    start_date = preference.start_date
    end_date = preference.end_date
    result_cities = {}
    for city in city_list:
        df = pd.read_csv("../Consumer_Airfare_Report__Table_1a_-_All_U.S._Airport_Pair_Markets.csv", low_memory=False)

        filtered_df = df[['city1', 'city2', 'airport_1', 'airport_2', 'fare']].copy()

        filtered_df['airport_1'] = filtered_df['airport_1'].str.strip().str.upper()
        filtered_df['airport_2'] = filtered_df['airport_2'].str.strip().str.upper()

        filtered_df['fare'] = pd.to_numeric(filtered_df['fare'], errors='coerce')
        data = filtered_df

        matching_rows = data[
            ((data['airport_1'] == curr_city.airport_code) & (data['airport_2'] == city.airport_code)) | (
                    (data['airport_1'] == city.airport_code) & (data['airport_2'] == curr_city.airport_code))]

        if matching_rows.empty:
            average_fare = 0
        else:
            average_fare = matching_rows['fare'].mean()

        if average_fare != 0:
            price_to = average_fare
            price_from = average_fare
        else:
            price_to = findFlightPrices(curr_city.airport_code, city.airport_code, start_date)
            # price_to = 120
            price_from = findFlightPrices(city.airport_code, curr_city.airport_code, end_date)
            # price_from = 110
        avg_cost = Cities.query.filter_by(city_name=city.city_name)[0].average_cost
        if price_to == 0 or price_from == 0:
            continue
        addFlightObjectToDatabase(curr_city.city_name, city.city_name, price_to, price_from, start_date, end_date)
        total_price = float(price_to) + float(price_from) + float(avg_cost) * float(num_days)
        actual_budget = float(preference.budget)
        if total_price <= actual_budget:
            result_cities[city] = round(total_price, 2)
    return result_cities

# def createTargetCities(city_list, curr_city, preference, num_days):
#     start_date = preference.start_date
#     end_date = preference.end_date
#     result_cities = {}
#     for city in city_list:
#         price_to = findFlightPrices(curr_city.airport_code, city.airport_code, start_date)
#         # price_to = 120
#         price_from = findFlightPrices(city.airport_code, curr_city.airport_code, end_date)
#         # price_from = 110
#         avg_cost = Cities.query.filter_by(city_name=city.city_name)[0].average_cost
#         if(price_to == 0 or price_from == 0):
#             continue
#         addFlightObjectToDatabase(curr_city.city_name, city.city_name, price_to, price_from, start_date, end_date)
#         total_price = float(price_to) + float(price_from) + float(avg_cost)*float(num_days)
#         actual_budget = float(preference.budget)
#         if(total_price <= actual_budget):
#             result_cities[city] = round(total_price,2)
#     return result_cities

def createCityList(latitude, longitude, max_distance):
    city_list = []
    max_lat, min_lat, max_long, min_long = get_square_bounds(latitude, longitude, max_distance)
    cities = Cities.query.filter(Cities.city_latitude<=max_lat).filter(Cities.city_latitude>=min_lat).filter(Cities.city_longitude<=max_long).filter(Cities.city_longitude>=min_long).all()

    if len(cities) < 10:
        city_list = cities
    else:
        city_list = random.sample(cities, 10)

    return city_list

def createCityRecommendation(city, budget):
    poi_list = []
    pois = Place_of_Interest.query.filter_by(city_id = city.id).all()
    for poi in pois:
        poi_list.append(poi.place_name)
    return format_city(city, poi_list, budget)
        
    
def createRecommendations(result_cities, preference, user):
    sorted_city_list = sorted(result_cities.items(), key=lambda x: x[1])
    city_poi_list = []
    cnt=0
    for key, value in sorted_city_list:
        if cnt==5:
            break
        cnt = cnt+1
        city = key
        budget = value

        curr_city_recommendation = createCityRecommendation(city, budget)
        city_poi_list.append(curr_city_recommendation)
    ts = datetime.now()
    recommendations_mongo.insert_one({'user': format_user(user), 'preference': format_preference(preference), 'city': city_poi_list, 'timestamp': ts})
    return city_poi_list

def calculateTrip(user, preference):
    location = preference.origin_city
    curr_city = Cities.query.filter_by(city_name=location)[0]

    origin_airport = curr_city.airport_code
    latitude = float(curr_city.city_latitude)
    longitude = float(curr_city.city_longitude)

    num_days = calculateNumDays(preference)

    max_distance = num_days * 700

    city_list = createCityList(latitude, longitude, max_distance)

    result_cities = createTargetCities(city_list, curr_city, preference, num_days)

    city_poi_list = createRecommendations(result_cities, preference, user)
    
    return city_poi_list

@app.route('/login', methods = ['POST'])
def create_user():
    name = request.json['name']
    email = request.json['email']
    person = Person(name, email)
    person_exists = None
    inspector = inspect(db.engine)
    table_name = 'person'
    if inspector.has_table(table_name):
        person_exists = db.session.query(Person).filter(Person.email == email).first()
        print("person = ", person_exists)
        print('Table Exists')
        if person_exists:
            if person_exists.name == name:
                print('Returning valid user')
                return format_user(person)

    if not person_exists:
        print('In not person exists')
        db.session.add(person)
        db.session.commit()
        return format_user(person)
    print('Returning null value')
    return ({'name': 'Not Valid'})

@app.route('/send_preferences', methods = ['POST'])
def get_preferences():
    name = request.json['name']
    email = request.json['email']
    budget = request.json['budget']
    start_date = request.json['startDate']
    end_date = request.json['endDate']
    origin_city = request.json['originCity']

    user = Person(name, email)

    preference = Preferences(budget, start_date, end_date, origin_city)
    past_search = get_past_searches(name, email)
    city_poi_list = calculateTrip(user, preference)
    curr_and_past_search = {"cities": city_poi_list, "past_search":past_search}
    return curr_and_past_search

def createJSONForCityObject(past_search):
    cities = []
    for i in range(len(past_search['city'])):
        city = past_search['city'][i]
        name = city[0]
        airport_code = city[1]
        city_id = city[2]
        latitude = city[3]
        longitude = city[4]
        poi = city[5]
        budget = city[6]
        cities.append(format_city_poi(name, airport_code, city_id, latitude, longitude, poi, budget))
    
    res = json.loads(json_util.dumps(cities))
    return {"city": res}

def get_past_searches(name, email):
    # city_objects = recommendations.filter(recommendations.user.name == name).filter(recommendations.user.email == email)
    print(recommendations.printSchema())
    city_objects = recommendations.filter(col('user.name') == name).filter(col('user.email')==email)
    city_objects_list = city_objects.collect()
    if len(city_objects_list) == 0:
        return {}
    elif len(city_objects_list) == 1:
        return {}
    else:
        row = city_objects_list[0]
        row_dict_first = row.asDict()
        time_1 = row_dict_first['timestamp']
        row_second = city_objects_list[1]
        row_dict = row_second.asDict()
        time_2 = row_dict['timestamp']
        if time_1 < time_2:
            max_time = time_2
            max_time_2 = time_1
            past_search = json.loads(json_util.dumps(row_dict_first))
        elif time_2 < time_1:
            max_time = time_1
            max_time_2 = time_2
            past_search = json.loads(json_util.dumps(row_dict))
        for i in range(len(city_objects_list)):
            row = city_objects_list[i]
            temp_row_dict = row.asDict()
            if row_dict['timestamp'] > max_time:
                past_search = json.loads(json_util.dumps(row_dict_first))
                row_dict = row_dict_first
                row_dict_first = temp_row_dict
                max_time_2 = max_time
                max_time = temp_row_dict['timestamp']
            elif max_time_2 < temp_row_dict['timestamp'] and temp_row_dict['timestamp'] < max_time:
                row_dict = temp_row_dict
                max_time_2 = row_dict['timestamp']
                past_search = json.loads(json_util.dumps(row_dict))
            
        return createJSONForCityObject(past_search)
    
@app.route('/get-cities', methods = ['GET'])
def get_cities():
    cities = Cities.query.all()
    city_list = []
    for city in cities:
        city_list.append({'label': city.city_name, 'value': city.city_name})

    print(city_list)
    return city_list

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)