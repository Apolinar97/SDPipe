import os
import csv
import tempfile
from urllib.request import urlretrieve
from datetime import date
from pipeline.staging.data_config import STAGING_DATASETS, StagingDataConfig
from pipeline.db import get_connection

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Environment variable {name} is required but not set.")
    return val

def _get_batch_size() -> int:
    raw = os.getenv("STAGING_BATCH_SIZE", "2000")
    try:
        batch_size = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"STAGING_BATCH_SIZE must be an integer, got: {raw!r}"
        ) from exc
    if batch_size <= 0:
        raise RuntimeError(
            f"STAGING_BATCH_SIZE must be > 0, got: {batch_size}"
        )
    return batch_size

# Returns a tuple of (source_file_path, source_file_name, is_temp_file)
def _resolve_source_file(config: StagingDataConfig) -> tuple[str, str, bool]:
    local_file_env = config.file_env_var
    local_file = os.getenv(local_file_env)
    if local_file:
        if not os.path.isfile(local_file):
            raise RuntimeError(
                f"{local_file_env} is set but file does not exist: {local_file}"
            )
        return local_file, os.path.basename(local_file), False

    source_url = _require_env(config.url_env_var)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name
    urlretrieve(source_url, tmp_path)
    source_file = os.path.basename(source_url.rstrip("/")) or config.name
    return tmp_path, source_file, True

def _validate_header(config: StagingDataConfig, fieldnames: list[str] | None) -> None:
    if not fieldnames:
        raise RuntimeError(f"No CSV header found for dataset {config.name}.")

    normalized_headers = [h.strip() for h in fieldnames if h is not None]
    duplicates = sorted({h for h in normalized_headers if normalized_headers.count(h) > 1})
    if duplicates:
        raise RuntimeError(
            f"Dataset {config.name} has duplicate header columns: {duplicates}"
        )

    expected = set(config.columns)
    actual = set(normalized_headers)

    missing_columns = sorted(expected - actual)
    if missing_columns:
        raise RuntimeError(
            f"Dataset {config.name} missing required columns: {missing_columns}"
        )

    unexpected_columns = sorted(actual - expected)
    # TODO: Add a non-strict mode to ignore unexpected columns when upstream adds extra fields.
    if unexpected_columns:
        raise RuntimeError(
            f"Dataset {config.name} has unexpected columns: {unexpected_columns}"
        )

def _normalize_row(config: StagingDataConfig, row: dict[str, str | None], snapshot_dt: date,source_file: str,row_num: int) -> tuple[object, ...]:
    row_values: list[object] = []
    for col in config.columns:
        raw = row.get(col)
        if raw is None:
            value: object = None
        else:
            trimmed = raw.strip()
            value = trimmed if trimmed != "" else None

        if value is None and col in config.required_columns:
            raise RuntimeError(
                f"Dataset {config.name} row {row_num}: required column {col} is missing/empty"
            )

        if value is not None and col in config.integer_columns:
            try:
                value = int(str(value))
            except ValueError as exc:
                raise RuntimeError(
                    f"Dataset {config.name} row {row_num}: invalid integer for {col}: {value!r}") from exc
            
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

def load_data_set(config: StagingDataConfig):
    source_path, source_file, is_temp_file = _resolve_source_file(config)
    snapshot_dt = date.today()
    batch_size = _get_batch_size()
    inserted_rows = 0

    try:
        with open(source_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            _validate_header(config, reader.fieldnames)
            insert_sql = _build_insert_sql(config)
            batch: list[tuple[object, ...]] = []
            with get_connection() as conn:
                with conn.cursor() as cur:
                    for row_num, row in enumerate(reader, start=2):
                        normalized_row = _normalize_row(
                            config=config,
                            row=row,
                            snapshot_dt=snapshot_dt,
                            source_file=source_file,
                            row_num=row_num,
                        )
                        batch.append(normalized_row)
                        if len(batch) >= batch_size:
                            inserted_rows += _flush_batch(cur, insert_sql, batch)

                    inserted_rows += _flush_batch(cur, insert_sql, batch)
            print(
                f"Loaded dataset {config.name} into {config.table_name} "
                f"({inserted_rows} rows, source={source_file})"
            )
    finally:
        if is_temp_file and os.path.exists(source_path):
            os.remove(source_path)

def main() -> None:
    for config in STAGING_DATASETS:
        load_data_set(config)

if __name__ == "__main__":
    main()
