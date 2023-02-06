from airflow import DAG
from airflow.utils.dates import days_ago

import random

with DAG(dag_id='example_dag_basic', start_date=days_ago(2), schedule=None, catchup=False) as dag:

    @dag.task
    def make_list():
        return [random.randint(1, 100) for i in range(random.randint(2, 5))]

    @dag.task
    def consumer(value):
        return value + 10

    consumer.expand(value=make_list())