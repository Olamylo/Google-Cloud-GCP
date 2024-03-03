from google.cloud import storage

def list_buckets(project_id, key_path):
    """Lists all buckets in the specified Google Cloud Storage project."""
    # Initialize the GCS client with explicit credentials
    client = storage.Client.from_service_account_json(key_path, project=project_id)

    # List all buckets
    buckets = client.list_buckets()

    # Print bucket names
    print("List of Buckets:")
    for bucket in buckets:
        print(bucket.name)

if __name__ == "__main__":
    # Replace 'your-project-id' with your actual GCP project ID
    project_id = "dbt-setup-356612"
    # Define the path to your service account key file
    key_path = "C:/Temp/GCP/dbt-setup-356612-xxxxxxx.json"
    list_buckets(project_id, key_path)
