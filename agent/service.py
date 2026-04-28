import client
import db


def is_blank(value: str | None) -> bool:
    return not value or value.strip() == ""


def get_sql(nl_query: str) -> str:
    return client.call_generate_sql(nl_query).get("sql_query")


def execute_query(sql: str) -> tuple[list[str], list[tuple]]:
    return db.execute_sql(sql)


def run_query(nl_query: str) -> tuple[str, list[str], list[tuple]]:
    sql = get_sql(nl_query)
    columns, rows = execute_query(sql)
    return sql, columns, rows
