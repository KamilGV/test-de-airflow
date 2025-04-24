from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'test_greenplum_connection',
        default_args=default_args,
        schedule_interval=None,  # Запуск только вручную
        catchup=False,
        tags=['test'],
) as dag:
    test_query = PostgresOperator(
        task_id='test_connection',
        postgres_conn_id='greenplum_conn',  # Используем ваше подключение
        sql="SELECT version();",  # Простой тестовый запрос
    )