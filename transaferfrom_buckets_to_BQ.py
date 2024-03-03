import os
from google.cloud import bigquery
from google.cloud import storage


def list_buckets_and_load_to_bq(project_id, key_path, dataset_id, table_name):
    # Initialize the GCS client with explicit credentials
    storage_client = storage.Client.from_service_account_json(key_path, project=project_id)

    # List all buckets
    buckets = storage_client.list_buckets()

    # Initialize BigQuery client with provided service account key
    bq_client = bigquery.Client.from_service_account_json(key_path, project=project_id)

    # Check if the table exists, delete it if it does
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_name)
        table = bq_client.get_table(table_ref)  # Fetch the table
        print(f"Table {dataset_id}.{table_name} exists: {table is not None}")

        # if table is None:
        #     print(f"Table {dataset_id}.{table_name} does not exist.")
        # else :
        #     bq_client.delete_table(table_ref)
        #     print(f"Deleted existing table {dataset_id}.{table_name} in BigQuery.")

    except Exception as e:
        print(f"Error: {e}")
        print(f"Table {dataset_id}.{table_name} does not exist.")

    for bucket in buckets:
        print(f"Processing Bucket: {bucket.name}, Location: {bucket.location}")
        # Get the blobs (files) from the bucket
        blobs = storage_client.list_blobs(bucket.name)
        location = bucket.location
        # lower case the location
        location = location.lower()
        print(f"Location: {location}")

        for blob in blobs:
            print(blob.name)
            # Assuming CSV files, you might want to add more filters if needed
            if blob.name.endswith('.csv'):
                print(f"Loading file: {blob.name} from Bucket: {bucket.name}")
                # Download the CSV file to local storage
                temp_local_file = f"/tmp/{blob.name}"  # Change the path as needed
                blob.download_to_filename(temp_local_file)

                # Construct the BigQuery table reference
                dataset_ref = bq_client.dataset(dataset_id)
                table_ref = dataset_ref.table(table_name)

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
    # Replace 'your-project-id' with your actual GCP project ID
    project_id = "dbt-setup-356612"
    # Define the path to your service account key file
    key_path = "C:/Temp/GCP/dbt-setup-356612-xxxxxxx.json"

    # Set your BigQuery dataset and table details
    dataset_id = "mylo_gcp_test_dataset"
    table_name = "STAGING_STOCKS_TABLE"

    # Call the function to list buckets and load CSV files from GCS to BigQuery
    list_buckets_and_load_to_bq(project_id, key_path, dataset_id, table_name)
