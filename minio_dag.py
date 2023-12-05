import pandas as pd
import requests
import json
import boto3

from datetime import datetime, timedelta, date
from pandas.io import parquet

from airflow import DAG, Dataset
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import *

from minio import Minio


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 5),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

minio_server = "http://10.0.2.15:9000"
minio_user = 'minioadmin'
minio_user_pass = 'minioadmin'
connection_timeout = "60000"

bucket_raw_data = 'rawdata'
conn_to_minio_from_airflow = 'my_minio'

today = datetime.now()
date_now = str(today.strftime("%Y%m%d%H%M"))

dag = DAG(
    'upload_to_minio_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
)


def get_session():
    spark = SparkSession.builder.appName("spark_app").getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_user)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_user_pass)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_server)
    return spark


def upload_to_minio_raw_data(**_):
    sites = {"bitfinex": "https://api.bitfinex.com/v1/trades/btcusd?limit_trades=500",
             "bitmex": "https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&count=500&reverse=true"}

    for site in sites:
        data = requests.get(sites[site]).json()
        s3 = S3Hook(conn_to_minio_from_airflow)
        if s3.check_for_bucket(bucket_raw_data):
            s3.load_string(
                string_data=json.dumps(data),
                key=f"{site}-{date_now}.json",
                bucket_name=bucket_raw_data
            )
        else:
            print(f"Bucket {bucket_raw_data} does not exist.")


upload_to_minio_task = PythonOperator(
    task_id='raw_data_to_minio',
    python_callable=upload_to_minio_raw_data,
    provide_context=True,
    dag=dag,
)


