import psycopg2
import argparse

conn_string1= ""
conn_string2 = ""

def _extract_from_table(table_name):
    with psycopg2.connect(conn_string1) as conn, conn.cursor() as cursor:
        q = f"COPY {table_name} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open(f'{table_name}_resultsfile.csv', 'w') as f:
            cursor.copy_expert(q, f)


def _write_to_table(table_name):
    with psycopg2.connect(conn_string2) as conn, conn.cursor() as cursor:
        q = f"COPY {table_name} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'{table_name}_resultsfile.csv', 'r') as f:
            cursor.copy_expert(q, f)

def main(table_name):
    _extract_from_table(table_name)
    _write_to_table(table_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_name', type=str, help='Please insert the name of the table')
    args = parser.parse_args()
    table_name = args.table_name
    main(table_name)

