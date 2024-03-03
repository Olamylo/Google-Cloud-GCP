import os
from google.cloud import bigquery
from google.cloud import storage

def load_csv_to_bq(gcs_bucket_name, gcs_file_name, dataset_id, table_name, project_id, service_account_key_path):
    # Initialize BigQuery client with provided service account key
    bq_client = bigquery.Client.from_service_account_json(service_account_key_path, project=project_id)

    # Initialize GCS client with provided service account key
    storage_client = storage.Client.from_service_account_json(service_account_key_path)

    # Get the GCS bucket
    bucket = storage_client.get_bucket(gcs_bucket_name)

    # Get the blob (file) from the bucket
    blob = bucket.blob(gcs_file_name)

    # Download the CSV file to local storage
    temp_local_file = "/tmp/temp_file.csv"  # You can change the path as needed
    blob.download_to_filename(temp_local_file)

    # Construct the BigQuery table reference
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)

    # Configure the job to load data from CSV file into BigQuery
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,  # If CSV file has headers, otherwise set to 0
        source_format=bigquery.SourceFormat.CSV
    )

    # Start the job to load data from CSV to BigQuery
    with open(temp_local_file, "rb") as source_file:
        job = bq_client.load_table_from_file(
            source_file,
            table_ref,
            location="europe-central2",  # Change this to match your dataset's location
            job_config=job_config
        )

    # Wait for the job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {dataset_id}.{table_name} in BigQuery.")

    # Clean up: Delete the temporary local file
    os.remove(temp_local_file)

if __name__ == "__main__":
    # Set your GCS bucket name, file name, BigQuery dataset and table details
    gcs_bucket_name = "mylo_test_bucket"
    gcs_file_name = "uploaded_file.csv"
    dataset_id = "mylo_gcp_test_dataset"
    table_name = "STAGING_STOCKS"

    # Set your Google Cloud project ID
    project_id = "dbt-setup-356612"


    # Provide the path to the service account key JSON file
    service_account_key_path = "C:/Temp/GCP/dbt-setup-356612-xxxxxxx.json"

    # Call the function to load CSV file from GCS to BigQuery
    load_csv_to_bq(gcs_bucket_name, gcs_file_name, dataset_id, table_name, project_id, service_account_key_path)
