import csv
import os
import pymysql

OUTPUT_DIR = "tables"

CONFIG = {
    "host": "relational.fel.cvut.cz",
    "port": 3306,
    "user": "guest",
    "password": "ctu-relational",
    "database": "tpch",
    "ssl": False,  # Prevents SSL errors with the remote server
}

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

if __name__ == "__main__":
    export_tpch_to_csv()