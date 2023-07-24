from extract import extract_data
from transform import transform_data
from load import load_data
from extract import con
from datetime import datetime

connection = con()


'''
In this main.py file I have called functions from other 3 scripts of extract, transform and load.
so that anyone can easily understand each of the stage of ETL pipeline.
It will also help anyone to refactor the code and to test & trace it.

'''


def run_etl():
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)

    try:
        load_data(transformed_data)
        with connection.cursor() as cursor:
            log_tbl = 'task_logs'
            tgt_schema = 'target_schema'
            task_name = 'run_etl'
            start_date = datetime.now()
            status = 'success'
            description = 'Run_ETL completed successfully.'
            end_date = datetime.now()

            cursor.execute(
                f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
            connection.commit()

        return "Run ETL has been completed."

    except Exception as e:
        with connection.cursor() as cursor:
            log_tbl = 'task_logs'
            tgt_schema = 'target_schema'
            task_name = 'run_etl'
            start_date = datetime.now()
            status = 'failed'
            description = 'Run_ETL has been failed.'
            end_date = datetime.now()

            cursor.execute(
                f"insert into {tgt_schema}.{log_tbl} values ('{task_name}','{start_date}','{end_date}','{description}','{status}');")
            connection.commit()

        return f"Unable to run the load due to error: \n{e}"


run_etl()
