from __future__ import annotations
import json
from pathlib import Path
import os
import time
import requests
from pydantic import ValidationError
from pipeline.storage.object_store import ObjectStore
from pipeline.config.object_store_config import ObjectStoreConfig
from pipeline.logging_config import configure_logging, get_logger
from pipeline.weather.models import BeatStationMapping, NwsStationObservation
from pipeline.weather.nws_api_fetcher import fetch_latest_observation

TEMP_DIR_ROOT = os.getenv('TEMP_DIR_ROOT', '/tmp')  # Default to /tmp if not set
object_store = None
logger = get_logger(__name__)

def require_env(var_name):
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable '{var_name}' is required but not set.")
    return value

def get_object_store_config() -> ObjectStoreConfig:
    s3_end_point = require_env('AWS_S3_ENDPOINT')
    s3_access_key = require_env('AWS_S3_ACCESS_KEY')
    s3_secret_key = require_env('AWS_S3_SECRET_KEY')
    s3_weather_bucket_name = require_env('AWS_S3_WEATHER_BUCKET_NAME')
    return ObjectStoreConfig(s3_end_point, s3_access_key, s3_secret_key, s3_weather_bucket_name)

def load_base_stations_mapping_file(object_store: ObjectStore, mapping_file_key: str) -> Path:
    local_path = Path(TEMP_DIR_ROOT) / mapping_file_key
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

def collect_station_observations(unique_station_set:set[str]) -> list[NwsStationObservation]:
    nws_observations : list[NwsStationObservation] = []
    failed_station_count = 0
    started_at = time.perf_counter()
    for station_id in unique_station_set:
        try:
            response = fetch_latest_observation(station_id)
            nws_observations.append(response)
            logger.info("Fetched latest observation: station_id=%s", station_id)
        except requests.exceptions.RequestException:
            failed_station_count += 1
            logger.warning("NWS request failed: station_id=%s", station_id, exc_info=True)
        except ValidationError:
            failed_station_count += 1
            logger.warning("NWS payload validation failed: station_id=%s", station_id, exc_info=True)
    elapsed_seconds = time.perf_counter() - started_at
    
    logger.info(
        "NWS collection complete: total_stations=%s successful_observations=%s failed_stations=%s elapsed_seconds=%.3f",
        len(unique_station_set),
        len(nws_observations),
        failed_station_count,
        elapsed_seconds,
    )
    return nws_observations

def lambda_handler(event, context):
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), service="pipeline.weather.nws_capture_lambda")
    global object_store
    if object_store is None:
        object_store_config = get_object_store_config()
        object_store = ObjectStore(object_store_config)
    mapping_file_key = require_env('MAPPING_FILE_KEY')
    mapping_file_path =load_base_stations_mapping_file(object_store, mapping_file_key)
    beat_to_station = build_beat_station_mapping(mapping_file_path)
    unique_station_set = get_unique_station_ids(beat_to_station)
    nws_observations: list[NwsStationObservation] = collect_station_observations(unique_station_set)
    for nws_obs in nws_observations:
        print(nws_obs)

if __name__ == "__main__":
    lambda_handler({}, None)
