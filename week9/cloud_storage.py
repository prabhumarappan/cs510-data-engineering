import os
from google.cloud import storage

client = storage.Client.from_service_account_json(json_credentials_path='credentials-python-storage.json')

bucket = client.get_bucket('ctran-archive-data')

def upload_file(file_path):
    file_name =os.path.basename(file_path)
    blob_object = bucket.blob(file_path)
    blob_object.upload_from_filename(file_name)
