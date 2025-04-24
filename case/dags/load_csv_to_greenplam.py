import pandas
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pandas as pd
import psycopg2
from pathlib import Path


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

def load_csv(path: str):
    df = pd.read_csv(path, sep=";")
    df.rename(columns={
        'Табельный номер': 'employee_id',
        'ФИО': 'full_name',
        'Подразделение': 'department',
        'Должность': 'position',
        'Дата рождения': 'birth_date',
        'Адрес': 'address',
        'Телефон 1': 'phone_1',
        'Телефон 2': 'phone_2',
        'Дата (месяц)': 'month',
        'Отработанное время': 'worked_hours'
    }, inplace=True)

    df['birth_date'] = pd.to_datetime(df['birth_date'], format='%d.%m.%Y').dt.date

    return df


def create_table_if_not_exists():
    conn = psycopg2.connect(
        dbname='postgres',
        user='gpadmin',
        password='greenplum',
        host='greenplum',
        port=5432
    )
    cur = conn.cursor()

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
            worked_hours INT,
            start_date DATE,
            end_date DATE,
            is_current BOOLEAN
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def load_csv_to_greenplam():
    path = Path(__file__).parent / 'data/t_employee.csv'
    data: pandas.DataFrame = load_csv(path)

    conn = psycopg2.connect(
        dbname='postgres',
        user='gpadmin',
        password='greenplum',
        host='greenplum',
        port=5432
    )

    cur = conn.cursor()
    for _, row in data.iterrows():
        cur.execute("""SELECT * FROM t_employee WHERE employee_id = %s AND is_current = TRUE""",
                    (row['employee_id'],))

        existing = cur.fetchone()

        if existing:
            changed = (
                    existing[1] != row['department'] or
                    existing[2] != row['position'] or
                    existing[4] != row['full_name'] or
                    existing[5] != row['birth_date'] or
                    existing[6] != row['address'] or
                    existing[7] != str(row['phone_1']) or
                    existing[8] != str(row['phone_2']) or
                    existing[9] != row['month'] or
                    existing[10] != row['worked_hours']
            )

            if changed:
                # Закрываем старую запись
                cur.execute("""
                        UPDATE t_employee
                        SET end_date = %s, is_current = FALSE
                        WHERE surrogate_id = %s
                    """, (date.today(), existing[0]))

                # Вставляем новую запись
                cur.execute("""
                        INSERT INTO t_employee (
                            department, position, employee_id, full_name, birth_date,
                            address, phone1, phone2, month, worked_hours,
                            start_date, end_date, is_current
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
                    """, (
                    row['department'], row['position'], row['employee_id'], row['full_name'],
                    row['birth_date'], row['address'], row['phone_1'], row['phone_2'],
                    row['month'], row['worked_hours'], date.today()
                ))
        else:
            # Вставка новой записи, если её нет
            cur.execute("""
                    INSERT INTO t_employee (
                        department, position, employee_id, full_name, birth_date,
                        address, phone1, phone2, month, worked_hours,
                        start_date, end_date, is_current
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, TRUE)
                """, (
                row['department'], row['position'], row['employee_id'], row['full_name'],
                row['birth_date'], row['address'], row['phone_1'], row['phone_2'],
                row['month'], row['worked_hours'], date.today()
            ))

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id='csv_to_greenplum_scd2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id='create_employee_table',
        python_callable=create_table_if_not_exists
    )

    load_data = PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_greenplam
    )

    create_table >> load_data
