import pandas as pd
import pymysql
from datetime import datetime
from config.config import dbconn
from insert_to_gbq import insert_to_gbq


def insert_data_to_db():
    # get connection from config
    conn = pymysql.connect(**dbconn)
    if conn.open:
        print("Connected to database")

    print("Reading csv file")
    df = pd.read_csv('RTDRS_20230323.csv')

    # (axis) exclude blank column (how) exclude blank row
    df = df.dropna(axis=1, how='all')

    # Replace EOF with NaT value to prevent conversion
    df.loc[df['RUN_TIME'] == 'EOF', 'RUN_TIME'] = pd.NaT

    # Convert run_time column to datetime object
    df['RUN_TIME'] = pd.to_datetime(df['RUN_TIME'])

    # Convert time_interval column to datetime object
    df['TIME_INTERVAL'] = pd.to_datetime(df['TIME_INTERVAL'])

    # filter out rows with NaT values
    df = df.loc[~df['RUN_TIME'].isna()]

    print("Inserting to database")
    affected_rows = 0
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute("""INSERT INTO Python_RTD (run_time, mkt_type, time_interval, region_name, resource_name, resource_type, commodity_type, sched_mw, price) VALUES( % s, % s, % s, % s, % s, % s, % s, % s, % s) ON DUPLICATE KEY UPDATE
                           sched_mw = VALUES(sched_mw),
                           price = VALUES(price)""",
                           (row[1], row[2], row[3], row[4],
                            row[5], row[6], row[7], row[8], row[9])
                           )
        affected_rows += cursor.rowcount
        # print(f"{affected_rows} rows affected")

    conn.commit()
    conn.close()
    if conn.close:
        print("Disconnected from database")

    # Check if there are rows added to table
    if (affected_rows != 0):
        print("SQL query is successful")

        csv_list = df.values.tolist()
        results = insert_to_gbq(bucket_name, csv_list,
                                path)
        return results
    else:
        print("No affected rows.")


bucket_name = "dev_dw_files"
bucket_folder = "python_uploads"
curr_date = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'RTDRS_20230323_{curr_date}.csv'
path = bucket_folder + '/' + filename

insert_data_to_db()
