import json
import os
# import ccloud_lib
import psycopg2
import datetime as dt

#establishing the connection
conn = psycopg2.connect(
   database="deproject", user='postgres', password='123456789', host='34.127.100.132', port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
cursor.execute("select version()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()
print("Connection established to: ",data)

data = {"EVENT_NO_TRIP": "167311280", "EVENT_NO_STOP": "167311285", "OPD_DATE": "07-SEP-20", "VEHICLE_ID": "2262", "METERS": "3", "ACT_TIME": "31607", "VELOCITY": "", "DIRECTION": "", "RADIO_QUALITY": "", "GPS_LONGITUDE": "-122.602233", "GPS_LATITUDE": "45.637882", "GPS_SATELLITES": "9", "GPS_HDOP": "1", "SCHEDULE_DEVIATION": ""}

def send_to_db(data):
    pass   

def insert_breadcrumb_data(data):
    sql = "INSERT INTO breadcrumb (tstamp, latitude, longitude, direction, speed,\ trip_id) VALUES (%s, %s, %s, %s, %s, %s)"

    act_time = int(data["ACT_TIME"])
    tstamp = dt.datetime.strptime(data["OPD_DATE"], "%d-%b-%y")
    tstamp = tstamp + dt.timedelta(seconds=act_time)
    latitude = float(data["GPS_LATITUDE"])
    longitude = float(data["GPS_LONGITUDE"])
    direction = 0
    speed = 0

    cursor.execute(sql, ())

def get_latest_trip_id():
    sql = "SE"
    
def insert_trip_data(data):
    sql = "INSERT INTO trip (vehicle_id, service_key) \
    VALUES (%s, %s) RETURNING trip_id;"

    vehicle_id = data["VEHICLE_ID"]
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

    cursor.execute(sql, (vehicle_id, service_key))
    trip_id = cursor.fetchone()[0]
    conn.commit()

    return trip_id
# how to generate speed and direction?
# how to generate route id?
# can we modify trip id to be of SERIAL type?


insert_trip_data(data)