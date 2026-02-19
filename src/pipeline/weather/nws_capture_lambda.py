from __future__ import annotations
import requests
import json
from pathlib import Path
from dataclasses import dataclass
import os
from pipeline.storage.object_store import ObjectStore
from pipeline.config.object_store_config import ObjectStoreConfig
from pipeline.weather.models import NwsStationObservation, BeatStationMapping

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

def unique_station_ids(beatStationMapping: BeatStationMapping) -> set:
    pass

def build_beat_station_mapping(mapping_file_path:Path)-> list[BeatStationMapping]:
    if not mapping_file_path.exists():
        raise FileNotFoundError(f"Mapping file not found at '{mapping_file_path}'")
    
    raw_data = json.loads(mapping_file_path.read_text())
    validated_data = [BeatStationMapping.model_validate(row) for row in raw_data]
    return validated_data

def get_unique_station_ids(list_of_bts: list[BeatStationMapping])-> set[str]:
    return {bts.station_id for bts in list_of_bts if bts.station_id}

def fetch_nws_observation_by_station_id(station_id:str, require_qc:bool=True ) -> NwsStationObservation:
    try:
        api_url = API_URL + f'/stations/{station_id}/observations/latest'
        params = {
            "require_qc":str(require_qc).lower()
        }
        response = requests.get(api_url,params=params,timeout=10)
        response.raise_for_status()
        response_json = response.json()
        observation = NwsStationObservation.model_validate(response_json)
        return observation
    except requests.RequestException as e:
        print(f'Error:{e}')

def lambda_handler(event, context):
    global object_store
    if object_store is None:
        object_store_config = get_object_store_config()
        object_store = ObjectStore(object_store_config)
    mapping_file_key = require_env('MAPPING_FILE_KEY')
    mapping_file_path =load_base_stations_mapping_file(object_store, mapping_file_key)
    beat_to_station = build_beat_station_mapping(mapping_file_path)
    unique_station_ids = get_unique_station_ids(beat_to_station)
    test_station = unique_station_ids.pop()
    val = fetch_nws_observation_by_station_id(test_station)
    print(val)
    
    
if __name__ == "__main__":
    lambda_handler({}, None)
