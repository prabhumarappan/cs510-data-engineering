#!/usr/bin/env python
from confluent_kafka import Producer, KafkaError
import json
import time
import random
import os
import datetime as dt
import ccloud_lib
import glob
import requests as r
import pandas as pd
from pandas import json_normalize


CTRAN_API = "http://www.psudataeng.com:8000/getBreadCrumbData"
KAFKA_TOPIC = "sensor-data"

class KafkaProducer:
    def __init__(self) -> None:
        # Read arguments and configurations and initialize
        args = ccloud_lib.parse_args()
        config_file = args.config_file
        topic = args.topic
        conf = ccloud_lib.read_ccloud_config(config_file)

        # Create Producer instance
        producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
        self.producer = Producer(producer_conf)

        # Create topic if needed
        ccloud_lib.create_topic(conf, topic)

        self.delivered_records = 0


    def acked(self, err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            self.delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
    
    def produce_data(self, topic, key, data):
        record_value = json.dumps(data)
        print("Producing record: {}\t{}".format(key, record_value))
        self.producer.produce(topic, key=key, value=record_value, on_delivery=self.acked)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

kproducer = KafkaProducer()

# Function to get the file name for the new file
def get_output_file_path():
    script_path = os.path.realpath(__file__)
    file_name = os.path.basename(__file__)
    script_folder = script_path.replace(file_name, '')
    data_folder = os.path.join(script_folder, 'data')

    if not os.path.isdir(data_folder):
        os.mkdir(data_folder)

    todays_date = dt.datetime.now().strftime("%m-%d-%Y")  
    new_file_name = todays_date + ".json"
    
    return new_file_name

# Check if file with the same name exists, in that case, change the name to 
def check_output_file(f_path):
    script_path = os.path.realpath(__file__)
    file_name = os.path.basename(__file__)
    script_folder = script_path.replace(file_name, '')
    data_folder = os.path.join(script_folder, 'data')

    just_file_name = f_path.replace('.json', '')
    matching_files = glob.glob("%s/%s*.json" % (data_folder, just_file_name))

    if len(matching_files) > 0:
        f_path = just_file_name + "_" + str(len(matching_files) + 1) + ".json"
        
    return f_path

def get_api_response():
    response = r.get(CTRAN_API)
    return response.json()

def create_json_file(f_path, data):
    script_path = os.path.realpath(__file__)
    file_name = os.path.basename(__file__)
    script_folder = script_path.replace(file_name, '')
    data_folder = os.path.join(script_folder, 'data')
    final_file_path = os.path.join(data_folder, f_path)

    fo = open(final_file_path, 'w')
    json.dump(data, fo)
    fo.close()

def send_to_kafka(data):
    for d in data:
        key = d["OPD_DATE"]
        kproducer.produce_data(KAFKA_TOPIC, key, d)
    kproducer.flush()

def verify_data(data):
    verifiedData=[]
    df=json_normalize(data)
    
    #OPD_DATE must exist
    if(df['OPD_DATE'].isnull().values.any()):
        df['OPD_DATE']=df['OPD_DATE'].interpolate(method='linear')


    #VEHICLE_ID must exist
    if(df['VEHICLE_ID'].isnull().values.any()):
        df['VEHICLE_ID']=df['VEHICLE_ID'].interpolate(method='linear')

    #DIRECTION must exist
    if(df['DIRECTION'].isnull().values.any()):
        df['DIRECTION']=df['DIRECTION'].interpolate(method='linear')


    #GPS_LONGITUDE must exist
    if(df['GPS_LONGITUDE'].isnull().values.any()):
        df['GPS_LONGITUDE']=df['GPS_LONGITUDE'].interpolate(method='linear')


    #GPS_LATITUDE must exist
    if(df['GPS_LATITUDE'].isnull().values.any()):
        df['GPS_LATITUDE']=df['GPS_LATITUDE'].interpolate(method='linear')


    #EVENT_NO_TRIP must exist
    if(df['EVENT_NO_TRIP'].isnull().values.any()):
        df['EVENT_NO_TRIP']=df['EVENT_NO_TRIP'].interpolate(method='linear')

    #ACT_TIME must exist
    if(df['ACT_TIME'].isnull().values.any()):
        df['ACT_TIME']=df['ACT_TIME'].interpolate(method='linear')


    #METERS must exist
    if(df['METERS'].isnull().values.any()):
        df['METERS']=df['METERS'].interpolate(method='linear')


    #VELOCITY must exist
    if(df['VELOCITY'].isnull().values.any()):
        df['VELOCITY']=df['VELOCITY'].interpolate(method='linear')


    #“Latitude and Longitude must present together”
    if(df['GPS_LONGITUDE'].isnull().values.any() and df['GPS_LATITUDE'].isnull().values.any()):
        df['GPS_LONGITUDE']=df['GPS_LONGITUDE'].interpolate(method='linear')
        df['GPS_LATITUDE']=df['GPS_LATITUDE'].interpolate(method='linear')

    result = df.to_json(orient="index")
    parsed = json.loads(result)
    json.dumps(parsed, indent=4)
    for i in parsed:
        verifiedData.append(parsed[i])

    return verifiedData

def fetch_data():
    f_path = get_output_file_path()
    f_path = check_output_file(f_path)
    data = get_api_response()
    data=verify_data(data)
    create_json_file(f_path, data)
    send_to_kafka(data)
    print("sent total of %d messages" % kproducer.delivered_records)
    print("File created successfully %s" % f_path)   

if __name__ == "__main__":
    fetch_data()
