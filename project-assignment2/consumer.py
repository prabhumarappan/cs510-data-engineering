#!/usr/bin/env python
# from confluent_kafka import Consumer
import json
import os
# import ccloud_lib
import psycopg2

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

#Closing the connection
conn.close()

script_path = os.path.realpath(__file__)
file_name = os.path.basename(__file__)
script_folder = script_path.replace(file_name, '')
FIXED_PATH = os.path.join(script_folder, "consumed")

if not os.path.isdir(FIXED_PATH):
    os.mkdir(FIXED_PATH)

def dump_file(data):
    trip_no = data["EVENT_NO_TRIP"]
    time = data["ACT_TIME"]
    date = data["OPD_DATE"]
    key = "%s-%s-%s.json" % (date, trip_no, time)
    file_path = os.path.join(FIXED_PATH, key)
    fo = open(file_path, 'a')
    json.dump(data, fo)
    fo.close()

# def send_to_db(data):
    

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
