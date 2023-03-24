from load_data import data_load
from upload_csv_to_bucket_from_string import upload_csv_to_bucket_from_string
from datetime import datetime
import io
import csv


def insert_to_gbq(bucket_name, bucket_data, path):
    bucket_name = bucket_name
    data = bucket_data
    path = path

    bucket_data = []
    for row in data:
        timestamps = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row.append(timestamps)
        row.append(timestamps)
        bucket_data.append(row)

    field_names = ['run_time', 'mkt_type', 'time_interval', 'region_name', 'resource_name', 'resource_type', 'commodity_type', 'sched_mw', 'price', 'created_at', 'gbq_date_created'
                   ]

    # CREATE CSV DATA TO STRING
    string_data = io.StringIO()
    csv_writer = csv.writer(string_data)
    csv_writer.writerow(field_names)
    csv_writer.writerows(bucket_data)
    csv_string = string_data.getvalue()

    # return message
    msgJSON = {
        'status': 'success'
    }
    results = upload_csv_to_bucket_from_string(
        bucket_name, csv_string, path, msgJSON)

    gcs_path = 'gs://' + bucket_name + '/' + path
    table_name = 'iemop_python_rtd_reserve_schedule'
    dataset_id = 'traning_dataset'

    if results['status'] == 'success':
        try:
            success_message = data_load(dataset_id, table_name, gcs_path)
        except AssertionError as error:
            print(f'Load job failed with error: {error}')
        else:
            print(success_message)
