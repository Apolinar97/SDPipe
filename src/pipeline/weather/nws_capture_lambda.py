from __future__ import annotations
import json
from pathlib import Path
import os
import time
from datetime import datetime, timezone
import requests
from typing import Any
from pipeline.storage.object_store import ObjectStore
from pipeline.config.object_store_config import ObjectStoreConfig
from pipeline.logging_config import configure_logging, get_logger
from pipeline.weather.models import BeatStationMapping
from pipeline.weather.nws_api_fetcher import create_nws_session, fetch_latest_observation_json

TEMP_DIR_ROOT = os.getenv('TEMP_DIR_ROOT', '/tmp')  # Default to /tmp if not set
TEMP_OBSERVATION_FILE_PREFIX = os.getenv('TEMP_OBSERVATION_FILE_PREFIX','nws_observations')
DATA_SOURCE = 'https://api.weather.gov'
SCHEMA_VERSION = 1

object_store: ObjectStore = None
logger = get_logger(__name__)

def require_env(var_name):
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable '{var_name}' is required but not set.")
    return value

def get_object_store_config() -> ObjectStoreConfig:
    s3_end_point = os.getenv('AWS_S3_ENDPOINT') or None
    s3_access_key = os.getenv('AWS_S3_ACCESS_KEY') or None
    s3_secret_key = os.getenv('AWS_S3_SECRET_KEY') or None
    s3_region = os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or None
    s3_weather_bucket_name = require_env('AWS_S3_WEATHER_BUCKET_NAME')
    return ObjectStoreConfig(
        bucket_name=s3_weather_bucket_name,
        endpoint=s3_end_point,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        region=s3_region,
    )

def load_base_stations_mapping_file(object_store: ObjectStore, mapping_file_key: str) -> Path:
    local_path = Path(TEMP_DIR_ROOT) / mapping_file_key
    local_path.parent.mkdir(parents=True, exist_ok=True)
    object_store.download_object(mapping_file_key, str(local_path))
    return local_path

def build_beat_station_mapping(mapping_file_path:Path)-> list[BeatStationMapping]:
    if not mapping_file_path.exists():
        raise FileNotFoundError(f"Mapping file not found at '{mapping_file_path}'")
    
    raw_data = json.loads(mapping_file_path.read_text())
    validated_data = [BeatStationMapping.model_validate(row) for row in raw_data]
    return validated_data

def get_unique_station_ids(list_of_bts: list[BeatStationMapping])-> set[str]:
    return {bts.station_id for bts in list_of_bts if bts.station_id}

def build_observation_batch(
    captured_at: datetime,
    observations: list[dict[str, Any]],
    stations_requested: set[str],
    failed_stations: set[str]
) -> dict[str, Any]:
    return {
        "captured_at_utc": captured_at.isoformat(),
        "stations_requested": sorted(stations_requested),
        "stations_failed": sorted(failed_stations),
        "source": DATA_SOURCE,
        "schema_version": SCHEMA_VERSION,
        "observations": observations
        
    }

def collect_station_observation_json(unique_station_set: set[str]) -> tuple[list[dict[str, Any]], set[str]]:
    nws_observation_json: list[dict[str, Any]] = []
    failed_stations: set[str] = set()
    started_at = time.perf_counter()
    session = create_nws_session()
    for station_id in unique_station_set:
        try:
            response = fetch_latest_observation_json(station_id, session=session)
            nws_observation_json.append(response)
            logger.info("Fetched latest observation json: station_id=%s", station_id)
        except requests.exceptions.RequestException:
            failed_stations.add(station_id)
            logger.warning("NWS request failed for raw json: station_id=%s", station_id, exc_info=True)
        except ValueError:
            failed_stations.add(station_id)
            logger.warning("NWS json parse failed: station_id=%s", station_id, exc_info=True)
    elapsed_seconds = time.perf_counter() - started_at

    logger.info(
        "NWS raw json collection complete: total_stations=%s successful_observations=%s failed_stations=%s elapsed_seconds=%.3f",
        len(unique_station_set),
        len(nws_observation_json),
        len(failed_stations),
        elapsed_seconds,
    )
    return nws_observation_json, failed_stations

def compute_weather_file_name(utc_time_prefix:datetime) -> str:
    time_stamp = utc_time_prefix.strftime("%Y-%m-%dT%H-%M-%SZ")
    return f'{TEMP_OBSERVATION_FILE_PREFIX}/{time_stamp}.json'


def lambda_handler(event, context):
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), service="pipeline.weather.nws_capture_lambda")
    global object_store
    try:
        if object_store is None:
            object_store_config = get_object_store_config()
            object_store = ObjectStore(object_store_config)
        mapping_file_key = require_env('MAPPING_FILE_KEY')
        mapping_file_path = load_base_stations_mapping_file(object_store, mapping_file_key)
        beat_to_station = build_beat_station_mapping(mapping_file_path)
        unique_station_set = get_unique_station_ids(beat_to_station)
        now_utc = datetime.now(timezone.utc)
        s3_file_name = compute_weather_file_name(now_utc)
        nws_observations_json, failed_stations = collect_station_observation_json(unique_station_set)
        
        s3_payload = build_observation_batch(
            captured_at= now_utc,
            observations=nws_observations_json,
            stations_requested= unique_station_set,
            failed_stations= failed_stations
        )
        json_bytes = json.dumps(s3_payload,default=str).encode('utf-8')
        object_store.put_object(s3_file_name,json_bytes)
        logger.info(f'Uploaded NWS Observation batch: key:{s3_file_name}, Observations: {nws_observations_json}, failed: {len(failed_stations)}')
    except Exception:
        logger.exception(f"Unhandled error in nws_capture_lambda")
        raise
if __name__ == "__main__":
    lambda_handler({}, None)
