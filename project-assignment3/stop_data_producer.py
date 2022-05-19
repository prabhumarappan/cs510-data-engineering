#!/usr/bin/env python
from confluent_kafka import Producer, KafkaError
import json
import os
import datetime as dt
import ccloud_lib
import glob
from crawler import crawl_stop_event_page


KAFKA_TOPIC = "stop-data"

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

def get_data_from_json_file():
    fo = open('05-01-2022.json')
    data = json.load(fo)
    fo.close()
    return data

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
        key = str(d["trip_id"])
        kproducer.produce_data(KAFKA_TOPIC, key, d)
    kproducer.flush()

def fetch_data():
    print("Starting Crawling")
    data = crawl_stop_event_page()
    print("Finished Crawling Data")
    # data = get_data_from_json_file()
    send_to_kafka(data)
    print("sent total of %d messages" % kproducer.delivered_records)

if __name__ == "__main__":
    fetch_data()