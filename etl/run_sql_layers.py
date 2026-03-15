import os
from pathlib import Path
import psycopg

DATABASE_URL = os.environ["SUPABASE_DB_URL"]

def run_sql_file(conn, filepath):
    sql = Path(filepath).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"Ejecutado correctamente: {filepath}")

def main():
    with psycopg.connect(DATABASE_URL) as conn:
        run_sql_file(conn, "sql/03_silver_layer.sql")
        run_sql_file(conn, "sql/04_gold_layer.sql")

if __name__ == "__main__":
    main()
