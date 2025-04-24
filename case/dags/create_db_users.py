from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def create_gp_db_and_user():
    conn = psycopg2.connect(
        dbname='postgres',
        user='gpadmin',
        password='gpadmin',
        host='greenplum',
        port='5432'
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""ALTER DEFAULT PRIVILEGES FOR ROLE gpadmin IN SCHEMA public REVOKE ALL ON TABLES FROM gp_user;""")
    cur.execute("""REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM gp_user;""")
    cur.execute("""REVOKE ALL PRIVILEGES ON SCHEMA public FROM gp_user;""")
    cur.execute("""REVOKE ALL PRIVILEGES ON DATABASE postgres FROM gp_user;""")
    cur.execute("DROP USER IF EXISTS  gp_user;")
    cur.execute("CREATE USER gp_user WITH PASSWORD 'gp_user';")
    cur.execute("GRANT CONNECT ON DATABASE postgres TO gp_user;")

    cur.execute("GRANT USAGE ON SCHEMA public TO gp_user;")
    cur.execute("GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO gp_user;")
    cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO gp_user;")

    cur.close()
    conn.close()


def create_pg_db_and_user():
    conn = psycopg2.connect(
        dbname='template1',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS employee_data_pg;")
    cur.execute("DROP USER IF EXISTS test_pg_user;")
    cur.execute("CREATE DATABASE employee_data_pg;")
    cur.execute("CREATE USER test_pg_user WITH PASSWORD 'test_pg_user';")
    cur.execute("GRANT CONNECT ON DATABASE employee_data_pg TO test_pg_user;")

    cur.close()
    conn.close()

    conn = psycopg2.connect(
        dbname='employee_data_pg',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("GRANT USAGE ON SCHEMA public TO test_pg_user;")
    cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO test_pg_user;")
    cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO test_pg_user;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS t_employee (
            surrogate_id SERIAL PRIMARY KEY,
            department TEXT,
            position TEXT,
            employee_id INT,
            full_name TEXT,
            birth_date DATE,
            address TEXT,
            phone1 TEXT,
            phone2 TEXT,
            month INT,
            worked_hours INT
        );
    """)

    cur.execute("""GRANT USAGE, SELECT, UPDATE ON SEQUENCE t_employee_surrogate_id_seq TO test_pg_user;""")

    cur.close()
    conn.close()

with DAG(
    dag_id='init_databases_and_users',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    init_gp = PythonOperator(
        task_id='create_gp_database_and_user',
        python_callable=create_gp_db_and_user
    )

    init_pg = PythonOperator(
        task_id='create_pg_database_and_user',
        python_callable=create_pg_db_and_user
    )

    init_gp >> init_pg
