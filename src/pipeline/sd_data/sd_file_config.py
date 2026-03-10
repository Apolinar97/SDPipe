import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
import requests
from pipeline.storage.object_store import ObjectStore
from pipeline.config.object_store_env import get_object_store_config
#https://seshat.datasd.org/traffic_collisions/pd_collisions_datasd.csv

KEY_PREFIX = os.getenv('SD_FILES_PREFIX', 'raw')

@dataclass(frozen=True)
class SdFileConfig:
    data_set_name: str
    url: str
    key_prefix: str
    @property
    def filename(self) -> str:
        return self.url.split("/")[-1]
    def get_object_key(self, source_date: date) -> str:
        return f"{self.key_prefix}/{source_date.strftime('%Y-%m-%d')}/{self.filename}"
    def get_source_last_modified(self) -> datetime:
        """Return the UTC datetime the source file was last modified via the Last-Modified header."""
        try:
            response = requests.head(self.url)
            response.raise_for_status()
            last_modified_str = response.headers.get('Last-Modified')
            if not last_modified_str:
                raise ValueError(f"No Last-Modified header returned from {self.url}")
            return datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=timezone.utc)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Could not connect to {self.url}") from e
        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Request to {self.url} timed out") from e
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"HTTP error occurred while checking {self.url}: {e}") from e
        except ValueError as e:
            raise ValueError(f"Could not parse Last-Modified header from {self.url}: {e}") from e

    def is_source_updated(self, last_downloaded: datetime.date) -> bool:
        return self.get_source_last_modified().date() > last_downloaded

@dataclass(frozen=True)
class SdFileManagerConfig:
    storage: ObjectStore = ObjectStore(get_object_store_config("AWS_S3_BUCKET_NAME"))
    
    collisions_basic_report: SdFileConfig = SdFileConfig(
        data_set_name="collisions_basic_report",
        url = "https://seshat.datasd.org/traffic_collisions/pd_collisions_datasd.csv",
        key_prefix= KEY_PREFIX        
    )
    collisions_details_report: SdFileConfig = SdFileConfig(
        data_set_name="collisions_details_report",
        url = "https://seshat.datasd.org/traffic_collision_details/pd_collisions_details_datasd.csv",
        key_prefix= KEY_PREFIX
    )
    police_beats_attributes: SdFileConfig  = SdFileConfig(
        data_set_name="police_beats_attributes_table",
        url = 'https://seshat.datasd.org/gis_police_beats/pd_beats_datasd.csv',
        key_prefix= KEY_PREFIX
    )

    def all_files(self) -> list[SdFileConfig]:
        return [self.collisions_basic_report, self.collisions_details_report, self.police_beats_attributes]

