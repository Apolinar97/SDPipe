# Executes SQL DDL statements to create the tables in the database. 
# This will be replaced by a more robust migration tool, since the current implementation will be in AWS as RedShift.

from __future__ import annotations
from pathlib import Path
import psycopg
from pipeline.db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIRS = [
    PROJECT_ROOT / "sql" / "ddl",
]

def get_sql_files():
    files = []
    for directory in SQL_DIRS:
        if directory.exists() and directory.is_dir():
            files.extend(sorted(directory.glob("*.sql")))
    return files

def execute_sql_file(conn: psycopg.Connection, path: Path):
    sql = path.read_text(encoding="utf-8")
    if not sql.strip():
        print(f"Warning: SQL file {path} is empty. Skipping.")
        return
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            print(f"Executed SQL file: {path}")
        except Exception as e:
            print(f"Error executing SQL file {path}: {e}")

def main():
    files = get_sql_files()
    if not files:
        print("No SQL files found in the specified directories.")
        return
    with get_connection() as conn:
        for file in files:
            rel = file.relative_to(PROJECT_ROOT)
            print(f"Executing SQL file: {rel}")
            execute_sql_file(conn, file)
    print("All SQL files executed successfully.")
if __name__ == "__main__":
    main()
