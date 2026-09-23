import csv
import os
import pymysql
import psycopg2
import psycopg2.extras

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

def export_tpch_to_postgres():
    _create_postgres_database_if_missing()

    print("Connecting to MySQL...")
    mysql_conn = pymysql.connect(**CONFIG)
    mysql_cursor = mysql_conn.cursor()

    print("Connecting to Postgres...")
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cursor = pg_conn.cursor()

    mysql_cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in mysql_cursor.fetchall()]

    print(f"Found {len(tables)} tables: {', '.join(tables)}\n")

    for table in tables:
        print(f"Exporting table: {table}...")

        mysql_cursor.execute(f"DESCRIBE `{table}`;")
        columns = [(row[0], row[1]) for row in mysql_cursor.fetchall()]
        column_names = [name for name, _ in columns]

        column_defs = ", ".join(
            f'"{name}" {_postgres_type_for(col_type)}' for name, col_type in columns
        )
        pg_cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        pg_cursor.execute(f'CREATE TABLE "{table}" ({column_defs});')

        mysql_cursor.execute(f"SELECT * FROM `{table}`;")
        rows = mysql_cursor.fetchall()

        if rows:
            insert_columns = ", ".join(f'"{name}"' for name in column_names)
            psycopg2.extras.execute_values(
                pg_cursor,
                f'INSERT INTO "{table}" ({insert_columns}) VALUES %s',
                rows,
            )

        pg_conn.commit()
        print(f"  -> Loaded {len(rows)} rows into Postgres table '{table}'")

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
    print("\nAll tables exported successfully from MySQL to Postgres!")