import io
import sql
import pandas as pd
# import requests
import json
import boto3

from datetime import datetime, timedelta, date
from pandas.io import parquet

from airflow import DAG, Dataset
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import *

from sqlalchemy import create_engine, text

from minio import Minio


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 5),
    'retries': 0,
    'retry_delay': timedelta(hours=1),
}

#MinIO credentials and configs
minio_server = "http://10.0.2.15:9000"
minio_user = 'minioadmin'
minio_user_pass = 'minioadmin'
connection_timeout = "60000"

#PostgreSQL credentials and configs
postgres_host = "http://localhost"
postgres_port = "5432"
postgres_db = "stoks"
postgres_user = "admin"
postrges_pass = "admin"
postgres_table = "audit.daily_data"
con_string = 'postgresql://admin:admin@127.0.0.1:5432/stocks'
db = create_engine(con_string)

today = datetime.now()
date_now = str(today.strftime("%Y%m%d%H%M"))
today_date = date.today()
yesterday_date = today_date - timedelta(days=1)
current_hour = int(today.strftime("%H"))
aggregated_data_date = today_date if current_hour != 0 else yesterday_date
previous_hour = current_hour - 1 if current_hour != 0 else 23

bucket_aggregated_data = 'aggregateddata'
conn_to_minio_from_airflow = 'my_minio'
conn_to_postgres_from_airflow = 'postgres_conn'


def get_session():
    spark = SparkSession.builder.appName("spark_app").getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_user)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_user_pass)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_server)
    return spark


dag = DAG(
    'minio_to_postgres',
    default_args=default_args,
    schedule_interval='@hourly',
)


def write_agg_data_to_postgres():
    spark = get_session()
    s3 = S3Hook(conn_to_minio_from_airflow)
    keys = s3.list_keys(bucket_aggregated_data)
    aggregated_data = []
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("hour", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("amount", DoubleType(), True),
        StructField("exchange", StringType(), True),
        StructField("type", StringType(), True)])

    for key in keys:
        if f"{aggregated_data_date}-{previous_hour}" in key:
            spark_df = (spark
                        .read
                        .schema(schema)
                        .parquet(f"s3a://{bucket_aggregated_data}/{key}")
                        )
            if spark_df:
                aggregated_data.append(spark_df.withColumn("source", lit(f"df_{key}")))

    merged_df = aggregated_data[0]

    for df in aggregated_data[1:]:
        merged_df = merged_df.union(df)

    agg_df = merged_df.groupBy("date", "hour", "exchange", "type", "price").agg(sum("amount").alias("amount"))
    columns = ["date", "hour", "exchange", "type", "price", "amount"]
    rows = agg_df.rdd.map(lambda row: tuple(row)).collect()
    postgres_hook = PostgresHook(postgres_conn_id=conn_to_postgres_from_airflow)
    postgres_hook.insert_rows(table=postgres_table, rows=rows, target_fields=columns)

    spark.stop()


write_agg_data_to_postgres = PythonOperator(
    task_id='write_agg_data_to_postgres',
    python_callable=write_agg_data_to_postgres,
    provide_context=True,
    dag=dag,
)
