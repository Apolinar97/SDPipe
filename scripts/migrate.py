# Executes SQL DDL statements to create the tables in the database. 
# This will be replaced by a more robust migration tool, since the current implementation will be in AWS as RedShift.

from __future__ import annotations
from pathlib import Path
import psycopg
from pipeline.db import get_connection
from pipeline.logging_config import configure_logging, get_logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIRS = [
    PROJECT_ROOT / "sql" / "ddl",
]

logger = get_logger(__name__)


def get_sql_files():
    files = []
    for directory in SQL_DIRS:
        if directory.exists() and directory.is_dir():
            files.extend(sorted(directory.glob("*.sql")))
    return files

def execute_sql_file(conn: psycopg.Connection, path: Path):
    sql = path.read_text(encoding="utf-8")
    if not sql.strip():
        logger.warning("Skipping empty SQL file: path=%s", path)
        return
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            logger.info("Executed SQL file: path=%s", path)
        except Exception:
            logger.exception("Error executing SQL file: path=%s", path)

def main():
    configure_logging(service="scripts.migrate")
    files = get_sql_files()
    if not files:
        logger.warning("No SQL files found in configured directories.")
        return
    with get_connection() as conn:
        for file in files:
            rel = file.relative_to(PROJECT_ROOT)
            logger.info("Executing SQL file: path=%s", rel)
            execute_sql_file(conn, file)
    logger.info("All SQL files executed.")
if __name__ == "__main__":
    main()
