import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import llm
import prompts


def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


CACHE_PATH = Path("/tmp/sdpipe_last_result.json")
MAX_ROWS = 50


def _build_user_message(nl_query, sql, columns, rows, question):
    total = len(rows)
    sample = rows[:MAX_ROWS]
    truncated = total > MAX_ROWS

    table_lines = ["\t".join(str(v) for v in row) for row in sample]
    table_text = "\n".join(["\t".join(columns)] + table_lines)

    parts = [
        f"Original question: {nl_query}",
        f"SQL used:\n{sql}",
        f"Results ({total} row(s) total{', showing first ' + str(MAX_ROWS) if truncated else ''}):\n{table_text}",
    ]
    if truncated:
        parts.append(f"Note: result set truncated to {MAX_ROWS} of {total} rows.")
    parts.append(f"Specific question: {question}" if question else "Please provide a general analysis.")
    return "\n\n".join(parts)


def analyze(nl_query, sql, columns, rows, question=None):
    system = prompts.build_analysis_prompt()
    user_message = _build_user_message(nl_query, sql, columns, rows, question)
    client = llm.get_llm_client()
    return client.generate(system=system, user_message=user_message)


def save_cache(nl_query, sql, columns, rows):
    CACHE_PATH.write_text(
        json.dumps(
            {
                "nl_query": nl_query,
                "sql": sql,
                "columns": columns,
                "rows": [list(r) for r in rows],
            },
            default=_json_default,
        )
    )


def load_cache():
    if not CACHE_PATH.exists():
        return None
    data = json.loads(CACHE_PATH.read_text())
    return data["nl_query"], data["sql"], data["columns"], [tuple(r) for r in data["rows"]]
