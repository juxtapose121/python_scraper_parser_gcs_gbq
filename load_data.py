from google.cloud import bigquery


def data_load(dataset_id, table_name, file_location):

    # Create bigquery client from service account key file
    client = bigquery.Client.from_service_account_json(
        'service_account_dw.json')

    # Create a reference of the table from dataset
    table_ref = client.dataset(dataset_id).table(table_name)

    # Create a job configuration
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1
    )

    # Create a job loader
    load_job = client.load_table_from_uri(
        file_location,
        table_ref,
        job_config=job_config
    )

    assert load_job.job_type == 'load'
    load_job.result()
    assert load_job.state == 'DONE'
    return f'Table {table_name} in dataset {dataset_id} was successfully loaded from {file_location}'
