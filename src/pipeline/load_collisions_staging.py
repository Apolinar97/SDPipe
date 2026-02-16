from __future__ import annotations
import os
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class StagingDataConfig:
    name: str
    table_name: str
    columns: tuple[str, ...]
    integer_columns: tuple[str, ...]
    timestamp_columns: tuple[str, ...]
    url_env_var: str

COLLISIONS_STAGING_BASIC = StagingDataConfig(
    name="collisions_staging_basic",
)
