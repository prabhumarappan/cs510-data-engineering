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


def insert_stop_data(data):
    sql = "INSERT INTO stop_event (trip_id, vehicle_id, leave_time, train, route_id, direction, stop_time, arrive_time, dwell, location_id, door, lift, ons, offs, estimated_load, maximum_distance, train_mileage, pattern_distance, location_distance, x_coordinate, y_coordinate, data_source, schedule_status) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    trip_id = data["trip_id"]
    vehicle_id = data["vehicle_number"]
    leave_time = data["leave_time"]
    train = data["train"]
    route_id = data["route_id"]
    direction = data["direction"]
    stop_time = data["stop_time"]
    arrive_time =data["arrive_time"]
    dwell = data["dwell"]
    location_id = data["location_id"]
    door = data["door"]
    lift = data["lift"]
    ons = data["ons"]
    offs = data["offs"]
    estimated_load = data["estimated_load"]
    maximum_distance = data["maximum distance"]
    train_mileage = data["train_mileage"]
    pattern_distance = data['pattern_distance']
    location_distance = data['location_distance']
    x_coordinate = data['x_coordinate']
    y_coordinate = data['y_coordinate']
    data_source = data['data_source']
    schedule_status = data['schedule_status']
    
    cursor.execute(sql, (trip_id, vehicle_id, leave_time, train, route_id, direction, stop_time, arrive_time, dwell, location_id, door, lift, ons, offs, estimated_load, maximum_distance, train_mileage, pattern_distance, location_distance, x_coordinate, y_coordinate, data_source, schedule_status))


def send_to_db(data):
    try:
        insert_stop_data(data)
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