import json
import os
import psycopg2
import psycopg2.extras
import datetime as dt
from config import config

params = config()

#establishing the connection
conn = psycopg2.connect(**params)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
cursor.execute("select version()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()
print("Connection established to: ",data)

fo = open('04-28-2022.json')
data = json.load(fo)
fo.close()

breadcrumb_datas = []
trip_datas = {}

def send_to_db(data):
    get_trips()
    try:
        for i, x in enumerate(data):
            insert_trip_data(x)
            create_breadcrumb_data(x)
        print("%s number of data points inserted" % i)
        insert_breadcrumb_data()
        conn.commit()
    except Exception as e:
        print(x)
        print("Error occured while inserting ", e)
        conn.rollback()

def create_breadcrumb_data(data):
    act_time = int(data["ACT_TIME"])
    trip_id = int(data["EVENT_NO_TRIP"])
    tstamp = dt.datetime.strptime(data["OPD_DATE"], "%d-%b-%y")
    tstamp = tstamp + dt.timedelta(seconds=act_time)
    latitude = float(data["GPS_LATITUDE"]) if data['GPS_LATITUDE'] else 0.0
    longitude = float(data["GPS_LONGITUDE"]) if data['GPS_LONGITUDE'] else 0.0
    direction = int(data['DIRECTION']) if data['DIRECTION'] else 0
    speed = float(data['VELOCITY']) if data['VELOCITY'] else 0.0

    breadcrumb_datas.append([tstamp, latitude, longitude, direction, speed, trip_id])

def insert_breadcrumb_data():
    sql = "INSERT INTO breadcrumb (tstamp, latitude, longitude, direction, speed, trip_id) VALUES (%s, %s, %s, %s, %s, %s)"
    
    psycopg2.extras.execute_batch(cursor, sql, breadcrumb_datas)

def get_trips():
    sql = "SELECT trip_id from trip"
    cursor.execute(sql)
    trips = cursor.fetchall()
    if trips:
        for trip in trips:
            trip_datas[trip[0]] = True

def check_trip_exists(data):
    trip_id = int(data["EVENT_NO_TRIP"])
    if trip_id in trip_datas:
        return True
    else:
        return False
 
def insert_trip_data(data):
    if not check_trip_exists(data):
        sql = "INSERT INTO trip (trip_id, vehicle_id, service_key) \
        VALUES (%s, %s, %s);"

        trip_id = int(data["EVENT_NO_TRIP"])
        vehicle_id = int(data["VEHICLE_ID"])
        act_time = int(data["ACT_TIME"])
        tstamp = dt.datetime.strptime(data["OPD_DATE"], "%d-%b-%y")
        tstamp = tstamp + dt.timedelta(seconds=act_time)
        weekday = tstamp.weekday()
        if weekday < 5:
            service_key = "Weekday"
        elif weekday == 5:
            service_key = "Saturday"
        else:
            service_key = "Sunday"
        
        trip_datas[trip_id] = True
        
        cursor.execute(sql, (trip_id, vehicle_id, service_key))


send_to_db(data)