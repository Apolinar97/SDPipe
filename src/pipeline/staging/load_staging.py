import os
import io
import csv
from datetime import date
from pipeline.staging.data_config import STAGING_DATASETS, StagingDataConfig
from pipeline.db import get_connection
from pipeline.logging_config import configure_logging, get_logger
from pipeline.storage.object_store import ObjectStore

logger = get_logger(__name__)

def _get_batch_size() -> int:
    raw = os.getenv("STAGING_BATCH_SIZE", "2000")
    try:
        batch_size = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"STAGING_BATCH_SIZE must be an integer, got: {raw!r}") from exc
    if batch_size <= 0:
        raise RuntimeError(f"STAGING_BATCH_SIZE must be > 0, got: {batch_size}")
    return batch_size

def _get_run_date() -> date:
    raw = os.getenv("STAGING_RUN_DATE")
    if not raw:
        return date.today()
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise RuntimeError(f"STAGING_RUN_DATE must be in YYYY-MM-DD format, got: {raw!r}") from exc

def _resolve_source_key(config: StagingDataConfig, run_date: date, store: ObjectStore) -> str:
    source_root = os.getenv("STAGING_SOURCE_ROOT", "").rstrip("/")
    if not source_root:
        raise RuntimeError("STAGING_SOURCE_ROOT is required for staging load.")
    key = f"{source_root}/{run_date.isoformat()}/{config.daily_file_name}"
    if not store.object_exists(key):
        raise RuntimeError(f"Source file not found for dataset {config.name!r}: bucket={store.bucket_name!r} key={key!r}")
    return key

def _validate_header(config: StagingDataConfig, fieldnames: list[str] | None) -> None:
    if not fieldnames:
        raise RuntimeError(f"No CSV header found for dataset {config.name}.")

    normalized_headers = [h.strip() for h in fieldnames if h is not None]
    duplicates = sorted({h for h in normalized_headers if normalized_headers.count(h) > 1})
    if duplicates:
        raise RuntimeError(f"Dataset {config.name} has duplicate header columns: {duplicates}")

    expected = set(config.columns)
    actual = set(normalized_headers)

    missing_columns = sorted(expected - actual)
    if missing_columns:
        raise RuntimeError(f"Dataset {config.name} missing required columns: {missing_columns}")

    unexpected_columns = sorted(actual - expected)
    # TODO: Add a non-strict mode to ignore unexpected columns when upstream adds extra fields.
    if unexpected_columns:
        raise RuntimeError(f"Dataset {config.name} has unexpected columns: {unexpected_columns}")

def _normalize_row(config: StagingDataConfig, row: dict[str, str | None], snapshot_dt: date, source_file: str, row_num: int) -> tuple[object, ...]:
    row_values: list[object] = []
    for col in config.columns:
        raw = row.get(col)
        if raw is None:
            value: object = None
        else:
            trimmed = raw.strip()
            value = trimmed if trimmed != "" else None

        if value is None and col in config.required_columns:
            raise RuntimeError(f"Dataset {config.name} row {row_num}: required column {col} is missing/empty")

        if value is not None and col in config.integer_columns:
            try:
                value = int(str(value))
            except ValueError as exc:
                raise RuntimeError(f"Dataset {config.name} row {row_num}: invalid integer for {col}: {value!r}") from exc

        row_values.append(value)

    row_values.append(snapshot_dt)
    row_values.append(source_file)
    return tuple(row_values)

def _build_insert_sql(config: StagingDataConfig) -> str:
    insert_columns = list(config.columns) + ["snapshot_dt", "source_file"]
    placeholders = ", ".join(["%s"] * len(insert_columns))
    return (
        f"INSERT INTO {config.table_name} "
        f"({', '.join(insert_columns)}) VALUES ({placeholders})"
    )

def _flush_batch(cur, sql: str, batch: list[tuple[object, ...]]) -> int:
    if not batch:
        return 0
    cur.executemany(sql, batch)
    flushed = len(batch)
    batch.clear()
    return flushed

def _truncate_table(cur, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    existing_rows = cur.fetchone()[0]
    cur.execute(f"TRUNCATE TABLE {table_name}")
    return existing_rows

def _preflight_resolve_sources(run_date: date, store: ObjectStore) -> dict[str, str]:
    resolved_keys: dict[str, str] = {}
    source_errors: list[str] = []

    for config in STAGING_DATASETS:
        try:
            resolved_keys[config.name] = _resolve_source_key(config, run_date, store)
        except Exception as exc:
            source_errors.append(f"{config.name}: {exc}")

    if source_errors:
        logger.error("Preflight failed: not all files available for run_date=%s", run_date.isoformat())
        for err in source_errors:
            logger.error("Missing/invalid source: %s", err)
        raise RuntimeError("Preflight source validation failed.")

    logger.info("Preflight passed: run_date=%s datasets=%s", run_date.isoformat(), len(resolved_keys))
    return resolved_keys

def load_data_set(config: StagingDataConfig, cur, run_date: date, store: ObjectStore, key: str) -> int:
    try:
        snapshot_dt = run_date
        source_file = key.split("/")[-1]
        batch_size = _get_batch_size()
        inserted_rows = 0

        logger.info("Starting dataset load: dataset=%s table=%s key=%s source_file=%s", config.name, config.table_name, key, source_file)

        stream = store.get_object_stream(key)
        text_stream = io.TextIOWrapper(stream, encoding="utf-8")
        reader = csv.DictReader(text_stream)
        _validate_header(config, reader.fieldnames)
        truncated_rows = _truncate_table(cur, config.table_name)
        logger.info("Truncated table before load: dataset=%s table=%s truncated_rows=%s", config.name, config.table_name, truncated_rows)
        insert_sql = _build_insert_sql(config)
        batch: list[tuple[object, ...]] = []
        for row_num, row in enumerate(reader, start=2):
            normalized_row = _normalize_row(config=config, row=row, snapshot_dt=snapshot_dt, source_file=source_file, row_num=row_num)
            batch.append(normalized_row)
            if len(batch) >= batch_size:
                inserted_rows += _flush_batch(cur, insert_sql, batch)

        inserted_rows += _flush_batch(cur, insert_sql, batch)
        logger.info("Completed dataset load: dataset=%s table=%s inserted_rows=%s source_file=%s", config.name, config.table_name, inserted_rows, source_file)
        return inserted_rows
    except Exception:
        logger.exception("Dataset load failed: dataset=%s table=%s", config.name, config.table_name)
        raise

def main() -> None:
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), service="pipeline.staging")
    run_date = _get_run_date()
    store = ObjectStore()
    total_inserted_rows = 0
    logger.info("Starting staging load: run_date=%s datasets=%s bucket=%s", run_date.isoformat(), len(STAGING_DATASETS), store.bucket_name)
    try:
        resolved_keys = _preflight_resolve_sources(run_date, store)
        with get_connection() as conn:
            with conn.cursor() as cur:
                for config in STAGING_DATASETS:
                    key = resolved_keys[config.name]
                    total_inserted_rows += load_data_set(config, cur, run_date, store, key)
        logger.info("Staging load committed: run_date=%s total_rows=%s", run_date.isoformat(), total_inserted_rows)
    except Exception:
        logger.exception("Staging load failed and rolled back: run_date=%s", run_date.isoformat())
        raise

if __name__ == "__main__":
    main()
