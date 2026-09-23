import csv
import os
import pymysql
import psycopg2
import psycopg2.extras
import psycopg2.pool

OUTPUT_DIR = "tables"

CONFIG = {
    "host": "relational.fel.cvut.cz",
    "port": 3306,
    "user": "guest",
    "password": "ctu-relational",
    "database": "tpch",
    "ssl": False,  # Prevents SSL errors with the remote server
}

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "dbname": "tpch",
}

# Rough MySQL -> Postgres type mapping, keyed by the MySQL type name
# reported in `DESCRIBE`/`information_schema` (lowercased, no size info).
MYSQL_TO_POSTGRES_TYPES = {
    "tinyint": "SMALLINT",
    "smallint": "SMALLINT",
    "mediumint": "INTEGER",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC",
    "float": "REAL",
    "double": "DOUBLE PRECISION",
    "bit": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "time": "TIME",
    "year": "INTEGER",
    "char": "TEXT",
    "varchar": "TEXT",
    "text": "TEXT",
    "tinytext": "TEXT",
    "mediumtext": "TEXT",
    "longtext": "TEXT",
    "blob": "BYTEA",
    "tinyblob": "BYTEA",
    "mediumblob": "BYTEA",
    "longblob": "BYTEA",
}


def _postgres_type_for(mysql_column_type):
    base_type = mysql_column_type.split("(")[0].lower()
    return MYSQL_TO_POSTGRES_TYPES.get(base_type, "TEXT")

def export_tpch_to_csv():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Connecting to database...")
    conn = pymysql.connect(**CONFIG)
    cursor = conn.cursor()

    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor.fetchall()]

    print(f"Found {len(tables)} tables: {', '.join(tables)}\n")

    for table in tables:
        print(f"Exporting table: {table}...")
        
        cursor.execute(f"SELECT * FROM `{table}`;")
        headers = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        filepath = os.path.join(OUTPUT_DIR, f"{table}.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)
            writer.writerows(rows)

        print(f"  -> Saved {len(rows)} rows to {filepath}")

    cursor.close()
    conn.close()
    print(f"\nAll tables exported successfully to '{OUTPUT_DIR}/'!")

def _create_postgres_database_if_missing():
    admin_config = {**POSTGRES_CONFIG, "dbname": "postgres"}
    target_db = POSTGRES_CONFIG["dbname"]

    conn = psycopg2.connect(**admin_config)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
    if cursor.fetchone() is None:
        print(f"Database '{target_db}' does not exist, creating it...")
        cursor.execute(f'CREATE DATABASE "{target_db}";')
    else:
        print(f"Database '{target_db}' already exists.")

    cursor.close()
    conn.close()

PAGE_SIZE = 5000
POOL_MIN_CONNECTIONS = 1
POOL_MAX_CONNECTIONS = 5


def _create_postgres_table(pool, table, columns):
    column_defs = ", ".join(
        f'"{name}" {_postgres_type_for(col_type)}' for name, col_type in columns
    )

    pg_conn = pool.getconn()
    try:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        pg_cursor.execute(f'CREATE TABLE "{table}" ({column_defs});')
        pg_conn.commit()
        pg_cursor.close()
    finally:
        pool.putconn(pg_conn)


def _insert_postgres_page(pool, table, column_names, rows):
    insert_columns = ", ".join(f'"{name}"' for name in column_names)

    pg_conn = pool.getconn()
    try:
        pg_cursor = pg_conn.cursor()
        psycopg2.extras.execute_values(
            pg_cursor,
            f'INSERT INTO "{table}" ({insert_columns}) VALUES %s',
            rows,
        )
        pg_conn.commit()
        pg_cursor.close()
    finally:
        pool.putconn(pg_conn)


def export_tpch_to_postgres():
    _create_postgres_database_if_missing()

    print("Connecting to MySQL...")
    mysql_conn = pymysql.connect(**CONFIG)
    mysql_cursor = mysql_conn.cursor()

    print("Creating Postgres connection pool...")
    pool = psycopg2.pool.SimpleConnectionPool(
        POOL_MIN_CONNECTIONS, POOL_MAX_CONNECTIONS, **POSTGRES_CONFIG
    )

    try:
        mysql_cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in mysql_cursor.fetchall()]

        print(f"Found {len(tables)} tables: {', '.join(tables)}\n")

        for table in tables:
            print(f"Exporting table: {table}...")

            mysql_cursor.execute(f"DESCRIBE `{table}`;")
            columns = [(row[0], row[1]) for row in mysql_cursor.fetchall()]
            column_names = [name for name, _ in columns]

            _create_postgres_table(pool, table, columns)

            # Server-side cursor: rows stream from MySQL as we fetch them
            # instead of the whole table being buffered client-side up front.
            stream_cursor = mysql_conn.cursor(pymysql.cursors.SSCursor)
            stream_cursor.execute(f"SELECT * FROM `{table}`;")

            total_rows = 0
            page_number = 1
            while True:
                page = stream_cursor.fetchmany(PAGE_SIZE)
                if not page:
                    break

                _insert_postgres_page(pool, table, column_names, page)
                total_rows += len(page)
                print(f"  -> Page {page_number}: loaded {len(page)} rows (total {total_rows})")
                page_number += 1

            stream_cursor.close()
            print(f"  -> Finished loading {total_rows} rows into Postgres table '{table}'")
    finally:
        pool.closeall()
        mysql_cursor.close()
        mysql_conn.close()

    print("\nAll tables exported successfully from MySQL to Postgres!")