import csv
import os
import re
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
    "dbname": "postgres",
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
    """`columns` is a list of (name, postgres_type) pairs, already resolved."""
    column_defs = ", ".join(f'"{name}" {pg_type}' for name, pg_type in columns)

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
            columns = [(row[0], _postgres_type_for(row[1])) for row in mysql_cursor.fetchall()]
            column_names = [name for name, _ in columns]

            _create_postgres_table(pool, table, columns)

            # Buffered read: the remote server enforces max_statement_time,
            # so we let MySQL finish the query quickly and page through the
            # already-fetched rows locally for the (slower) Postgres inserts.
            mysql_cursor.execute(f"SELECT * FROM `{table}`;")
            rows = mysql_cursor.fetchall()

            total_rows = 0
            page_number = 1
            for offset in range(0, len(rows), PAGE_SIZE):
                page = rows[offset:offset + PAGE_SIZE]

                _insert_postgres_page(pool, table, column_names, page)
                total_rows += len(page)
                print(f"  -> Page {page_number}: loaded {len(page)} rows (total {total_rows})")
                page_number += 1

            print(f"  -> Finished loading {total_rows} rows into Postgres table '{table}'")
    finally:
        pool.closeall()
        mysql_cursor.close()
        mysql_conn.close()

    print("\nAll tables exported successfully from MySQL to Postgres!")

CSV_TYPE_SAMPLE_SIZE = 100
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _infer_postgres_type(values):
    non_empty = [v for v in values if v != ""]
    if not non_empty:
        return "TEXT"

    if all(DATE_PATTERN.match(v) for v in non_empty):
        return "DATE"

    try:
        for v in non_empty:
            int(v)
        return "BIGINT"
    except ValueError:
        pass

    try:
        for v in non_empty:
            float(v)
        return "DOUBLE PRECISION"
    except ValueError:
        pass

    return "TEXT"


def _infer_csv_columns(filepath):
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        sample_rows = []
        for row in reader:
            sample_rows.append(row)
            if len(sample_rows) >= CSV_TYPE_SAMPLE_SIZE:
                break

    column_types = [
        _infer_postgres_type([row[i] for row in sample_rows])
        for i in range(len(headers))
    ]
    return list(zip(headers, column_types))


def export_csv_to_postgres():
    """Load the CSVs produced by `export_tpch_to_csv` into Postgres.

    Reading from MySQL and writing to Postgres are fully decoupled here (no
    connection to either server is held open across the other's work), which
    avoids the MySQL `max_statement_time` timeout that a live MySQL->Postgres
    pipeline can hit while Postgres inserts are in progress.
    """
    _create_postgres_database_if_missing()

    csv_files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv"))
    if not csv_files:
        print(f"No CSV files found in '{OUTPUT_DIR}/'. Run export_tpch_to_csv() first.")
        return

    print("Creating Postgres connection pool...")
    pool = psycopg2.pool.SimpleConnectionPool(
        POOL_MIN_CONNECTIONS, POOL_MAX_CONNECTIONS, **POSTGRES_CONFIG
    )

    try:
        for csv_file in csv_files:
            table = csv_file[:-len(".csv")]
            filepath = os.path.join(OUTPUT_DIR, csv_file)
            print(f"Loading table: {table}...")

            columns = _infer_csv_columns(filepath)
            column_names = [name for name, _ in columns]
            _create_postgres_table(pool, table, columns)

            with open(filepath, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # skip header

                total_rows = 0
                page_number = 1
                page = []
                for row in reader:
                    page.append([value if value != "" else None for value in row])
                    if len(page) >= PAGE_SIZE:
                        _insert_postgres_page(pool, table, column_names, page)
                        total_rows += len(page)
                        print(f"  -> Page {page_number}: loaded {len(page)} rows (total {total_rows})")
                        page_number += 1
                        page = []

                if page:
                    _insert_postgres_page(pool, table, column_names, page)
                    total_rows += len(page)
                    print(f"  -> Page {page_number}: loaded {len(page)} rows (total {total_rows})")

            print(f"  -> Finished loading {total_rows} rows into Postgres table '{table}'")
    finally:
        pool.closeall()

    print("\nAll CSV files loaded successfully into Postgres!")

DUMP_FILE = "pg_dump.sql"


def _copy_escape(value):
    """Escape a value for Postgres COPY text format; "" is treated as NULL."""
    if value == "":
        return "\\N"
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def export_csv_to_pg_dump(dump_path=DUMP_FILE):
    """Turn the CSVs produced by `export_tpch_to_csv` into a plain-text
    Postgres dump (CREATE TABLE + COPY ... FROM stdin blocks), the same
    shape `pg_dump --format=plain` produces. No database connection is
    needed at all to generate it; load it later with:

        psql -h localhost -U postgres -d tpch -f pg_dump.sql
    """
    csv_files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv"))
    if not csv_files:
        print(f"No CSV files found in '{OUTPUT_DIR}/'. Run export_tpch_to_csv() first.")
        return

    with open(dump_path, "w", encoding="utf-8") as out:
        for csv_file in csv_files:
            table = csv_file[:-len(".csv")]
            filepath = os.path.join(OUTPUT_DIR, csv_file)
            print(f"Writing table: {table}...")

            columns = _infer_csv_columns(filepath)
            column_defs = ", ".join(f'"{name}" {pg_type}' for name, pg_type in columns)
            column_names = ", ".join(f'"{name}"' for name, _ in columns)

            out.write(f'DROP TABLE IF EXISTS "{table}" CASCADE;\n')
            out.write(f'CREATE TABLE "{table}" ({column_defs});\n')
            out.write(f'COPY "{table}" ({column_names}) FROM stdin;\n')

            total_rows = 0
            with open(filepath, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # skip header
                for row in reader:
                    out.write("\t".join(_copy_escape(value) for value in row) + "\n")
                    total_rows += 1

            out.write("\\.\n\n")
            print(f"  -> Wrote {total_rows} rows for table '{table}'")

    print(f"\nDump written to '{dump_path}'. Load it with:")
    print(f"  psql -h {POSTGRES_CONFIG['host']} -U {POSTGRES_CONFIG['user']} -d {POSTGRES_CONFIG['dbname']} -f {dump_path}")