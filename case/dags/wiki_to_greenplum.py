from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from tools import get_conn_greenplum

URL = "https://ru.wikipedia.org/w/api.php?action=query&list=allpages&aplimit=500&apfrom=A&format=json"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}


def get_wiki_data():
    response = requests.get(URL)
    response.raise_for_status()
    return response.json()


def create_table_if_not_exists():
    conn = get_conn_greenplum()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wiki_api (
            page_id INT PRIMARY KEY,
            ns INT,
            title TEXT);
    """)
    conn.commit()
    cur.close()
    conn.close()


def load_wiki_to_greenplam():
    data = get_wiki_data()

    conn = get_conn_greenplum()
    cur = conn.cursor()

    for row in data['query']['allpages']:
        cur.execute("""
            UPDATE wiki_api
            SET ns = %s, title = %s
            WHERE page_id = %s
        """, (
            row['ns'],
            row['title'],
            row['pageid'],
        ))

        cur.execute("""
            INSERT INTO wiki_api (page_id, ns, title)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM wiki_api WHERE page_id = %s
            )
        """, (
            row['pageid'],
            row['ns'],
            row['title'],
            row['pageid'],
        ))

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id='wiki_to_greenplum',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id='create_wiki_table',
        python_callable=create_table_if_not_exists
    )

    load_data = PythonOperator(
        task_id='load_wiki_to_greenplum',
        python_callable=load_wiki_to_greenplam
    )

    create_table >> load_data
