from google.cloud import bigquery


def create_blank_table(dataset_id, table_name, schema, partition_field):
    client = bigquery.Client.from_service_account_json(
        'service_account_dw.json')
    table = bigquery.Table(
        client.dataset(dataset_id)
        .table(table_name),
        schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field
    )
    table = client.create_table(table)
    print("Created table {}, partitioned on column {}".format(
        table.table_id, table.time_partitioning.field))

# END OF FUNCTION


schema = [
    bigquery.SchemaField('run_time', 'DATETIME'),
    bigquery.SchemaField('mkt_type', 'STRING'),
    bigquery.SchemaField('time_interval', 'DATETIME'),
    bigquery.SchemaField('region_name', 'STRING'),
    bigquery.SchemaField('resource_name', 'STRING'),
    bigquery.SchemaField('resource_type', 'STRING'),
    bigquery.SchemaField('commodity_type', 'STRING'),
    bigquery.SchemaField('sched_mw', 'FLOAT'),
    bigquery.SchemaField('price', 'FLOAT'),
    bigquery.SchemaField('created_at', 'DATETIME'),
    bigquery.SchemaField('gbq_date_created', 'DATETIME')
]
table_name = 'iemop_python_rtd_reserve_schedule'
dataset_id = 'traning_dataset'
partition_field = 'run_time'

print("Creating table")
create_blank_table(dataset_id, table_name, schema, partition_field)
