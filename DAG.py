from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from main import run_etl
import pendulum

local_tz = pendulum.local_timezone()

# default arguments for the DAG
default_args = {'owner':'Ajay',
                'description':'RUN ETL DAG',
                'depends_on_past': False,
                'retries':1,
                'retry_delay': timedelta(minutes=5)
                }

DAG_NAME = "RUN_ETL"

# Creating DAG object with scheduling
dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 19 * * *',
    start_date=datetime(2023, 7, 23, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1
)

# START TASK
init = EmptyOperator(
    task_id='START',
    dag=dag
)

# Run ETL task
run_etl_task = PythonOperator(
    task_id='run_etl_task',
    python_callable=run_etl,
    dag=dag
)

# END TASK
end_task = EmptyOperator(
    task_id='END',
    dag=dag,
)

# defining task dependencies using bit shift operator
init >> run_etl_task >> end_task
