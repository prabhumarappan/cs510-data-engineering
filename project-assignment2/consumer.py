#!/usr/bin/env python
from confluent_kafka import Consumer
import json
import os
import ccloud_lib
from config import config
import psycopg2
import psycopg2.extras
import datetime as dt

params = config()

#establishing the connection
conn = psycopg2.connect(**params)

cursor = conn.cursor()

cursor.execute("select version()")

data = cursor.fetchone()
print("Connection established to: ",data)

script_path = os.path.realpath(__file__)
file_name = os.path.basename(__file__)
script_folder = script_path.replace(file_name, '')
FIXED_PATH = os.path.join(script_folder, "consumed")

if not os.path.isdir(FIXED_PATH):
    os.mkdir(FIXED_PATH)

trip_datas = {}

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

def insert_breadcrumb_data(data):
    sql = "INSERT INTO breadcrumb (tstamp, latitude, longitude, direction, speed, trip_id) VALUES (%s, %s, %s, %s, %s, %s)"

    act_time = int(data["ACT_TIME"])
    trip_id = int(data["EVENT_NO_TRIP"])
    tstamp = dt.datetime.strptime(data["OPD_DATE"], "%d-%b-%y")
    tstamp = tstamp + dt.timedelta(seconds=act_time)
    latitude = float(data["GPS_LATITUDE"]) if data['GPS_LATITUDE'] else 0.0
    longitude = float(data["GPS_LONGITUDE"]) if data['GPS_LONGITUDE'] else 0.0
    direction = int(data['DIRECTION']) if data['DIRECTION'] else 0
    speed = float(data['VELOCITY']) if data['VELOCITY'] else 0.0

    cursor.execute(sql, (tstamp, latitude, longitude, direction, speed, trip_id))

def send_to_db(data):
    try:
        insert_trip_data(data)
        insert_breadcrumb_data(data)
        conn.commit()
    except Exception as e:
        print("Error inserting ", e)
        print(data)
        conn.rollback()

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0

    # Get existing trips from the DB
    get_trips()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                total_count += 1
                send_to_db(data)
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value, total_count))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

#Closing the connection
conn.close()