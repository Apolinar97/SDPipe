from __future__ import annotations
import requests
import json
from pathlib import Path
from dataclasses import dataclass
import os
from pipeline.storage.object_store import ObjectStore
from pipeline.config.object_store_config import ObjectStoreConfig
from pipeline.weather.models import NwsStationObservation

TEMP_DIR_ROOT = os.getenv('TEMP_DIR_ROOT', '/tmp')  # Default to /tmp if not set
API_URL = 'https://api.weather.gov'
object_store = None

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

def lambda_handler(event, context):
    global object_store
    if object_store is None:
        object_store_config = get_object_store_config()
        object_store = ObjectStore(object_store_config)
    mapping_file_key = require_env('MAPPING_FILE_KEY')
    mapping_file_path =load_base_stations_mapping_file(object_store, mapping_file_key)
    if not mapping_file_path.exists():
        raise FileNotFoundError(f"Mapping file not found at '{mapping_file_path}'")
    with open(mapping_file_path, 'r') as f:
        mapping_data = json.load(f)
    print(f"Loaded mapping data with {len(mapping_data)} entries from '{mapping_file_path}'")

if __name__ == "__main__":
    lambda_handler({}, None)