import os
from google.cloud import storage
import pandas as pd

def upload_to_gcs(csv_file_path, bucket_name, destination_blob_name, key_file_path):
    # Set the path to the service account key file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

    # Initialize a GCS client
    client = storage.Client()

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # Create a blob object
    blob = bucket.blob(destination_blob_name)

    # Upload the CSV file
    blob.upload_from_filename(csv_file_path)

    print(f"File {csv_file_path} uploaded to {bucket_name}/{destination_blob_name}.")


if __name__ == "__main__":
    path = 'C:/Temp/GCP/Datasets2'

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.csv'):
                print(file)
                csv_file_path = f"{path}/{file}"
                bucket_name = "mylo_test_bucket2"
                destination_blob_name = file
                key_file_path = "C:/Temp/GCP/dbt-setup-356612-xxxxxxx.json"

                upload_to_gcs(csv_file_path, bucket_name, destination_blob_name, key_file_path)
                print(f"File {file} uploaded to Google Cloud Storage.")

