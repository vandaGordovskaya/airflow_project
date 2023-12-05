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

today = datetime.now()
date_now = str(today.strftime("%Y%m%d%H%M"))
today_date = date.today()
yesterday_date = today_date - timedelta(days=1)
current_hour = int(today.strftime("%H"))
aggregated_data_date = today_date if current_hour != 0 else yesterday_date
previous_hour = current_hour - 1 if current_hour != 0 else 23

bucket_raw_data = 'rawdata'
bucket_unified_data = 'unifieddata'
bucket_aggregated_data = 'aggregateddata'
conn_to_minio_from_airflow = 'my_minio'

dag = DAG(
    'agg_data_in_minio',
    default_args=default_args,
    schedule_interval='@hourly',
)


def get_session():
    spark = SparkSession.builder.appName("spark_app").getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_user)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_user_pass)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_server)
    return spark


def delete_transfered_raw_data(list_files):
    s3 = S3Hook(conn_to_minio_from_airflow)
    s3.delete_objects(bucket=bucket_raw_data, keys=list_files)


def unify_and_transform_to_parquet():
    spark = get_session()
    s3 = S3Hook(conn_to_minio_from_airflow)
    keys = s3.list_keys(bucket_raw_data)
    success_load = 0

    for key in keys:
        file_name = key.split('.')
        file_name = file_name[0]
        columns = ["timestamp", "price", "amount", "exchange", "type"] if 'bitfinex' in file_name \
            else ["timestamp", "price", "size", "side"]
        spark_df = (spark
                    .read
                    .json(f"s3a://{bucket_raw_data}/{key}")
                    .select(columns))
        if 'bitmex' in file_name:
            spark_df = (spark_df.withColumnRenamed("size", "amount")
                        .withColumnRenamed("side", 'type'))
            spark_df = (spark_df.withColumn("exchange", lit('bitmex'))
                        .withColumn("timestamp", col("timestamp").cast("timestamp")))
            spark_df = spark_df.withColumn("amount", col("amount").cast("double"))
            new_order = ["timestamp", "price", "amount", "exchange", "type"]
            spark_df = spark_df.select(*new_order)
        if 'bitfinex' in file_name:
            spark_df = (spark_df.withColumn("price", col("price").cast("double"))
                        .withColumn("amount", col("amount").cast("double"))
                        .withColumn("timestamp", col("timestamp").cast("timestamp")))
        spark_df.printSchema()
        spark_df.write.parquet(
            f's3a://{bucket_unified_data}/{aggregated_data_date}/{previous_hour}/{file_name}.parquet')
        print(f"Uploaded {file_name} to MinIO.")
        success_load += 1

    spark.stop()
    if len(keys) == success_load:
        print("Transferred files will be deleted.")
        delete_transfered_raw_data(keys)


def aggregate_data_hourly():
    spark = get_session()
    s3 = S3Hook(conn_to_minio_from_airflow)
    keys = s3.list_keys(bucket_unified_data)
    aggregated_data = []
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("price", DoubleType(), True),
        StructField("amount", DoubleType(), True),
        StructField("exchange", StringType(), True),
        StructField("type", StringType(), True)])

    for key in keys:
        print(key)
        if '_SUCCESS' not in key:
            spark_df = (spark
                        .read
                        .schema(schema)
                        .parquet(f"s3a://{bucket_unified_data}/{key}")
                        )
            if spark_df:
                aggregated_data.append(spark_df.withColumn("source", lit(f"df_{key}")))

    merged_df = aggregated_data[0]

    for df in aggregated_data[1:]:
        merged_df = merged_df.union(df)

    merged_df = merged_df.withColumn("date", lit(aggregated_data_date))
    merged_df = merged_df.withColumn("hour", lit(previous_hour))

    agg_df = merged_df.groupBy("date", "hour", "exchange", "type", "price").agg(sum("amount").alias("amount"))
    file_name = f"agg-data-{aggregated_data_date}-{previous_hour}"
    agg_df.write.parquet(f's3a://{bucket_aggregated_data}/{file_name}.parquet')

    spark.stop()


unify_and_transform_to_parquet_task = PythonOperator(
    task_id='unify_and_transform_to_parquet',
    python_callable=unify_and_transform_to_parquet,
    provide_context=True,
    dag=dag,
)


aggregate_data = PythonOperator(
    task_id='aggregate_data_hourly',
    python_callable=aggregate_data_hourly,
    provide_context=True,
    dag=dag,
)

unify_and_transform_to_parquet_task >> aggregate_data
