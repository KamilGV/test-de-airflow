from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}


def migrate_data():
    gp_conn = psycopg2.connect(
        dbname='postgres',
        user='gp_user',
        password='gp_user',
        host='greenplum',
        port='5432'
    )
    gp_cur = gp_conn.cursor()
    gp_cur.execute("SELECT * FROM t_employee WHERE is_current = TRUE;")
    rows = gp_cur.fetchall()

    pg_conn = psycopg2.connect(
        dbname='employee_data_pg',
        user='test_pg_user',
        password='test_pg_user',
        host='postgres',
        port='5432'
    )
    pg_cur = pg_conn.cursor()

    pg_cur.execute("DELETE FROM t_employee;")
    for row in rows:
        print(row)
        pg_cur.execute("""
                        INSERT INTO t_employee (
                            department, position, employee_id, full_name, birth_date,
                            address, phone1, phone2, month, worked_hours
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                       (
                           row[1], row[2], row[3], row[4],row[5],
                           row[6], row[7], row[8],row[9], row[10]
                       ))

    pg_conn.commit()
    gp_cur.close()
    gp_conn.close()
    pg_cur.close()
    pg_conn.close()


with DAG(
        dag_id='migrate_gp_to_pg',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    migrate_task = PythonOperator(
        task_id='migrate_gp_to_postgres',
        python_callable=migrate_data
    )

    migrate_task
