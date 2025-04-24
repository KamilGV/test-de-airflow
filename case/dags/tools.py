import psycopg2


def get_conn_greenplum():
    return psycopg2.connect(
        dbname='postgres',
        user='gpadmin',
        password='greenplum',
        host='greenplum',
        port=5432
    )