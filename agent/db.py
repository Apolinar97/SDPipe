import os

import psycopg2


def execute_sql(sql: str) -> tuple[list[str], list[tuple]]:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5433)),
        dbname=os.getenv("DB_NAME", "sdpwarehouse"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "admin"),
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                raise ValueError("The SQL query did not return any results.")
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()
    return columns, rows
