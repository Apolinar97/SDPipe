from __future__ import annotations

import csv
import io
import json
import os
from datetime import date, datetime, timedelta, timezone

from pipeline.config.object_store_env import get_object_store_config
from pipeline.logging_config import configure_logging, get_logger
from pipeline.storage.object_store import ObjectStore
from pipeline.weather.models import NwsStationObservation, QuantifiedValue

logger = get_logger(__name__)

CSV_COLUMNS = [
    "station_id",
    "station_name",
    "observation_timestamp",
    "latitude",
    "longitude",
    "text_description",
    "temperature_c",
    "wind_direction_deg",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "visibility_m",
    "precip_last_1h_mm",
    "precip_last_3h_mm",
    "precip_last_6h_mm",
    "cloud_cover",
    "cloud_base_m",
    "captured_at_utc",
]

NWS_PREFIX = os.getenv("NWS_OBSERVATIONS_PREFIX", "")


def safe_value(qv: QuantifiedValue | None) -> float | None:
    """Return the numeric value, or None if missing or QC == 'Z'."""
    if qv is None:
        return None
    if qv.value is None:
        return None
    if qv.quality_control == "Z":
        return None
    return qv.value


def flatten_observation(obs: NwsStationObservation, captured_at_utc: str) -> dict:
    """Flatten a parsed observation into target warehouse columns."""
    props = obs.properties
    first_cloud = props.cloud_layers[0] if props.cloud_layers else None

    return {
        "station_id": props.station_id,
        "station_name": props.station_name,
        "observation_timestamp": props.timestamp.isoformat(),
        "latitude": obs.geometry.coordinates[1],
        "longitude": obs.geometry.coordinates[0],
        "text_description": props.text_description,
        "temperature_c": safe_value(props.temperature),
        "wind_direction_deg": safe_value(props.wind_direction),
        "wind_speed_kmh": safe_value(props.wind_speed),
        "wind_gust_kmh": safe_value(props.wind_gust),
        "visibility_m": safe_value(props.visibility),
        "precip_last_1h_mm": safe_value(props.precipitation_last_hour),
        "precip_last_3h_mm": safe_value(props.precipitation_last_3_hours),
        "precip_last_6h_mm": safe_value(props.precipitation_last_6_hours),
        "cloud_cover": first_cloud.amount if first_cloud else None,
        "cloud_base_m": first_cloud.base.value if first_cloud and first_cloud.base else None,
        "captured_at_utc": captured_at_utc,
    }


def flatten_batch_file(stream, file_key: str) -> list[dict]:
    """Read a batch JSON file from an S3 stream and return flattened observation rows."""
    raw_bytes = stream.read()
    batch = json.loads(raw_bytes)

    captured_at_utc = batch.get("captured_at_utc", "")
    raw_observations = batch.get("observations", [])

    rows: list[dict] = []
    for i, raw_obs in enumerate(raw_observations):
        try:
            obs = NwsStationObservation.model_validate(raw_obs)
            rows.append(flatten_observation(obs, captured_at_utc))
        except Exception:
            logger.exception(
                "Failed to parse observation %d in %s, skipping",
                i,
                file_key,
            )

    logger.info("Flattened %d observations from %s", len(rows), file_key)
    return rows


def deduplicate_observations(rows: list[dict]) -> list[dict]:
    """Deduplicate by (station_id, observation_timestamp), keeping latest captured_at_utc.

    Preserves all distinct hourly readings. Only collapses duplicate captures of
    the same observation (i.e. the same station + timestamp appearing in multiple
    batch files).
    """
    best: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (row["station_id"], row["observation_timestamp"])
        existing = best.get(key)
        if existing is None or row["captured_at_utc"] > existing["captured_at_utc"]:
            best[key] = row

    return sorted(best.values(), key=lambda r: (r["station_id"], r["observation_timestamp"]))


def build_csv_bytes(rows: list[dict]) -> bytes:
    """Serialize rows to CSV bytes with a fixed column order."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _parse_date(raw: str | None, env_var: str) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"{env_var} must be in YYYY-MM-DD format, got: {raw!r}"
        ) from exc


def _get_date_range() -> tuple[date, date]:
    today = datetime.now(timezone.utc).date()
    start = _parse_date(os.getenv("NWS_FLATTEN_RUN_DATE"), "NWS_FLATTEN_RUN_DATE") or today
    end = _parse_date(os.getenv("NWS_FLATTEN_END_DATE"), "NWS_FLATTEN_END_DATE") or start
    if end < start:
        raise RuntimeError(
            f"NWS_FLATTEN_END_DATE ({end}) must be >= NWS_FLATTEN_RUN_DATE ({start})"
        )
    return start, end


def _flatten_date(run_date: date, weather_store: ObjectStore, main_store: ObjectStore) -> bool:
    """Flatten all batch files for a single date. Returns True on success."""
    run_date_str = run_date.strftime("%Y-%m-%d")

    prefix = f"{NWS_PREFIX}/{run_date_str}/" if NWS_PREFIX else f"{run_date_str}/"
    batch_keys = weather_store.list_objects(prefix=prefix)

    if not batch_keys:
        logger.warning("No batch files found under %s in weather bucket — skipping", prefix)
        return False

    logger.info("Found %d batch files under %s", len(batch_keys), prefix)

    all_rows: list[dict] = []
    for key in batch_keys:
        stream = weather_store.get_object_stream(key)
        all_rows.extend(flatten_batch_file(stream, key))

    deduped = deduplicate_observations(all_rows)

    if not deduped:
        logger.warning("No observations after deduplication for %s — skipping", run_date_str)
        return False

    output_key = f"raw/{run_date_str}/nws_observations.csv"
    main_store.put_object(output_key, build_csv_bytes(deduped), content_type="text/csv")

    logger.info(
        "Flattened: run_date=%s raw_rows=%d deduped_rows=%d output=%s",
        run_date_str,
        len(all_rows),
        len(deduped),
        output_key,
    )
    return True


def main() -> None:
    configure_logging(service="pipeline.weather.nws_flattener")

    start_date, end_date = _get_date_range()
    total_days = (end_date - start_date).days + 1

    logger.info(
        "NWS observation flattener starting: start=%s end=%s total_days=%d",
        start_date,
        end_date,
        total_days,
    )

    weather_store = ObjectStore(get_object_store_config("AWS_S3_WEATHER_BUCKET_NAME"))
    main_store = ObjectStore(get_object_store_config("AWS_S3_BUCKET_NAME"))

    succeeded: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []

    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        try:
            if _flatten_date(current, weather_store, main_store):
                succeeded.append(date_str)
            else:
                skipped.append(date_str)
        except Exception:
            logger.exception("Failed to flatten date=%s — continuing", date_str)
            failed.append(date_str)
        current += timedelta(days=1)

    logger.info(
        "NWS flattener complete: succeeded=%d skipped=%d failed=%d",
        len(succeeded),
        len(skipped),
        len(failed),
    )
    if skipped:
        logger.warning("Skipped (no data): %s", ", ".join(skipped))
    if failed:
        logger.error("Failed dates: %s", ", ".join(failed))


if __name__ == "__main__":
    main()
