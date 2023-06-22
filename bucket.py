# from datetime import timedelta
# import datetime
import os
import glob
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/tb-intern02/Desktop/kafka/tb-sandbox-338012-58990b0cc3ec.json"

def create_bucket(bucket_name, storage_class='STANDARD', location='us-central1'): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = storage_class
   
    bucket = storage_client.create_bucket(bucket, location=location) 
    
    return f'Bucket {bucket.name} successfully created.'


def upload_cs_file(bucket_name, source_file_name, destination_file_name): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)
    print("data uploaded")
    return True

GCS_CLIENT = storage.Client()
def upload_folder(directory_path: str, dest_bucket_name: str, dest_blob_name: str):
    rel_paths = glob.glob(directory_path + '/**', recursive=True)
    bucket = GCS_CLIENT.get_bucket(dest_bucket_name)
    for local_file in rel_paths:
        remote_path = f'{dest_blob_name}/{"/".join(local_file.split(os.sep)[1:])}'
        if os.path.isfile(local_file):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_file)


def download_cs_file(bucket_name, file_name, destination_file_name): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.download_to_filename(destination_file_name)

    return True


def list_cs_files(bucket_name): 
    storage_client = storage.Client()

    file_list = storage_client.list_blobs(bucket_name)
    file_list = [file.name for file in file_list]

    return file_list


# def get_cs_file_url(bucket_name, file_name, expire_in=datetime.today() + timedelta(1)): 
#     storage_client = storage.Client()

#     bucket = storage_client.bucket(bucket_name)
#     url = bucket.blob(file_name).generate_signed_url(expire_in)

#     return url

# upload_folder("kafka-testing-bucket", "/home/tb-intern02/Desktop/kafka/test", "test")
# print(list_cs_files("kafka-testing-bucket"))