import psycopg2
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "poke_interval": 600
}

with DAG(
    dag_id="data_inserts",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as dag:

    dummy_start = DummyOperator(task_id='start')

    dummy_end = DummyOperator(task_id='end')

    def data_inserts_func():
        TABLE_NAMES = ['customer', 'lineitem', 'nation', 'orders',
                       'part', 'partsupp', 'region', 'supplier']
        conn_string1 = ""
        conn_string2 = ""

        for table in TABLE_NAMES:
            with psycopg2.connect(conn_string1) as conn, conn.cursor() as cursor:
                q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
                with open(f'{table}_resultsfile.csv', 'w') as f:
                    cursor.copy_expert(q, f)

            with psycopg2.connect(conn_string2) as conn, conn.cursor() as cursor:
                q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
                with open(f'{table}_resultsfile.csv', 'r') as f:
                    cursor.copy_expert(q, f)

    data_inserts = PythonOperator(
        task_id='extract_insert_data',
        python_callable=data_inserts_func,
        provide_context=True
    )

    dummy_start >> data_inserts >> dummy_end