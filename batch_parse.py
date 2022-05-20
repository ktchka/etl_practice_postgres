import psycopg2


TABLE_NAMES = ['customer', 'lineitem', 'nation', 'orders',
               'part', 'partsupp', 'region', 'supplier']

conn_string1= ""
conn_string2 = ""

def main():
    for table in TABLE_NAMES:
        with psycopg2.connect(conn_string1) as conn, conn.cursor() as cursor:
            q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            with open(f'{table}_resultsfile.csv', 'w') as f:
                cursor.copy_expert(q, f)

        with psycopg2.connect(conn_string2) as conn, conn.cursor() as cursor:
            q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
            with open(f'{table}_resultsfile.csv', 'r') as f:
                cursor.copy_expert(q, f)

if __name__ == '__main__':
    main()

