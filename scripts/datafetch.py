# Script to fetch and store C-Tran data in the same folder where the script is stored
# Downloaded data will be stored in JSON format and files will be names as per date
import glob
import os
import datetime as dt
import requests as r
import json

CTRAN_API = "http://www.psudataeng.com:8000/getBreadCrumbData"

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

def fetch_data():
    f_path = get_output_file_path()
    f_path = check_output_file(f_path)
    data = get_api_response()
    create_json_file(f_path, data)
    print("File created successfully %s" % f_path)

if __name__ == "__main__":
    fetch_data()    

# Questions: 
# 1. will the data be refreshed exactly at 12am?