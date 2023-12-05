import pandas as pd
import io

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta, date


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_email_dag',
    default_args=default_args,
    description='DAG to send daily emails with PostgreSQL data as CSV attachment',
    schedule_interval=None,
    # schedule_interval='@daily',
)

today = datetime.now()
today_date = date.today()
yesterday_date = today_date - timedelta(days=1)
conn_to_postgres_from_airflow = 'postgres_conn'


def extract_data_to_csv():
    postgres_query = f"SELECT * FROM audit.daily_data WHERE date = '{yesterday_date}'"
    pg_hook = PostgresHook(postgres_conn_id=conn_to_postgres_from_airflow)
    results = pg_hook.get_pandas_df(sql=postgres_query)

    csv_buffer = io.StringIO()
    results.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    csv_filename = '/tmp/postgres_data.csv'
    with open(csv_filename, 'w') as csv_file:
        csv_file.write(csv_data)

    return csv_filename


email_task = EmailOperator(
    task_id='send_daily_email',
    to='vgordovskaya@gmail.com',
    subject=f'Daily Data Export({yesterday_date})',
    html_content='Please find the attached CSV file with daily data.',
    files=['/tmp/postgres_data.csv'],
    dag=dag,
)


extract_data_task = PythonOperator(
    task_id='extract_data_to_csv',
    python_callable=extract_data_to_csv,
    dag=dag,
)


extract_data_task >> email_task
