import pandas as pd
import json
from datetime import datetime
import psycopg2


def con():
    # creating connection with postgres DB using psycopg2 library
    try:
        connection = psycopg2.connect(database='postgres',
                                      user='postgres',
                                      password='5254',
                                      host='localhost',
                                      port='5432')
        print(f"DB connected {connection}")
        return connection

    except psycopg2.Error as e:
        print("Unable to connect")


def extract_data():
    connection = con()
    try:
        # defining json config file path
        json_file_path = "C:\\Users\\vijay gautam\\Downloads\\json_config\\config.json"

        # reading json config to get the paths of input CSVs
        with open(json_file_path, 'r') as f:
            config = json.load(f)
            dir_path = config['config']['dir_path']
            cust_file_nm = config['config']['cust_file_name']
            prod_file_nm = config['config']['prod_cat_file_name']
            transactions_file_nm = config['config']['tranx_file_name']

        # reading CSV into pandas DF
        customer_df = pd.read_csv(dir_path+cust_file_nm)
        prod_cat_df = pd.read_csv(dir_path+prod_file_nm)
        transactions_df = pd.read_csv(dir_path+transactions_file_nm)

        # logging the task logs on success into the task_logs table
        with connection.cursor() as cursor:
            log_tbl = 'task_logs'
            tgt_schema = 'target_schema'
            task_name = 'extract'
            start_date = datetime.now()
            status = 'success'
            description = 'Data has been extracted and loaded to DF successfully.'
            end_date = datetime.now()

            print("entered extract")
            cursor.execute(f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
            connection.commit()
            print("logs inserted")

        return customer_df, prod_cat_df, transactions_df

    except Exception as e:
        # logging the task logs on failure into the task_logs table
        with connection.cursor() as cursor:

            task_name = 'extract'
            start_date = datetime.now()
            status = 'failed'
            description = "Unable to extract the data."
            end_date = datetime.now()
            log_tbl = 'task_logs'
            tgt_schema = 'target_schema'
            cursor.execute(
                f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
            connection.commit()
            print("logs inserted")
        return f"Error: unable to extract data due to following error- \n{e}"
