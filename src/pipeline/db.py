from __future__ import annotations
import os
import psycopg

def get_connection() -> psycopg.Connection:
    return psycopg.connect(host=os.getenv("DB_HOST", "127.0.0.1"), port=int(os.getenv("DB_PORT", "5433")), dbname=os.getenv("DB_NAME", "sdpwarehouse"), user=os.getenv("DB_USER", "postgres"), password=os.getenv("DB_PASSWORD", "admin"))
