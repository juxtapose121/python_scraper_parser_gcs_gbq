from google.cloud import storage


def upload_csv_to_bucket_from_string(bucket_name, file_name, path, msg):

    # Create a storage client using a service account key file
    storage_client = storage.Client.from_service_account_json(
        'service_account_dw.json')

    # Get a reference to the bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Create a blob object and upload the CSV file to the specified destination
    blob = bucket.blob(path)

    if blob.exists():
        print("File exists")
    else:
        blob.upload_from_string(file_name, content_type='text/csv')
        # Print a success message
        print(f'Successfully uploaded {path} to {bucket_name}')
        return msg
