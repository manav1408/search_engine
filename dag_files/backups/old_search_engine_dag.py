import os
from datetime import datetime

from product_indexing import fetch_products
from pySparkJob import gcs_to_mongo

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="search_engine-airflow",
    start_date=datetime(2023, 2, 22),
    schedule_interval="@daily",
) as dag:

    os.environ["no_proxy"] = "*"

    get_products_op = PythonOperator(
        task_id="get_products_data", python_callable=fetch_products
    )

    gcs_to_mongo_op = PythonOperator(task_id='gcs_to_mongo_data',
                                     python_callable=gcs_to_mongo)

    get_products_op >> gcs_to_mongo_op