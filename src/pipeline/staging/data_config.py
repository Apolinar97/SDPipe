from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class StagingDataConfig:
    name: str
    table_name: str
    url_env_var: str
    file_env_var: str
    columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    integer_columns: tuple[str, ...] = ()
    timestamp_columns: tuple[str, ...] = ()
    
COLLISIONS_STAGING_BASIC = StagingDataConfig(
    name="staging_collisions_basic",
    table_name="staging.collisions_basic",
    url_env_var="COLLISIONS_BASIC_URL",
    file_env_var="COLLISIONS_BASIC_FILE",
    columns = (
        "report_id",
        "date_time",
        "police_beat",
        "address_no_primary",
        "address_pd_primary",
        "address_road_primary",
        "address_sfx_primary",
        "address_pd_intersecting",
        "address_name_intersecting",
        "address_sfx_intersecting",
        "violation_section",
        "violation_type",
        "charge_desc",
        "injured",
        "killed",
        "hit_run_lvl",
    ),
    required_columns=(
        "report_id",
        "date_time",
        "police_beat",
        "address_no_primary",
    ),
    integer_columns=(
        "police_beat", "injured","killed"
    ),
    timestamp_columns=("date_time",)
)

COLLISIONS_STAGING_DETAILS = StagingDataConfig(
    name="staging_collisions_details",
    table_name="staging.collisions_details",
    url_env_var="COLLISIONS_DETAILS_URL",
    file_env_var="COLLISIONS_DETAILS_FILE",
    columns=(
        "report_id",
        "date_time",
        "person_role",
        "person_injury_lvl",
        "person_veh_type",
        "veh_type",
        "veh_make",
        "veh_model",
        "police_beat",
        "address_no_primary",
        "address_pd_primary",
        "address_road_primary",
        "address_sfx_primary",
        "address_pd_intersecting",
        "address_name_intersecting",
        "address_sfx_intersecting",
        "violation_section",
        "violation_type",
        "charge_desc",
        "injured",
        "killed",
        "hit_run_lvl",
    ),
    required_columns=(
        "report_id",
        "date_time",
        "police_beat",
    ),
    integer_columns=(
        "police_beat", "injured","killed"
    ),
    timestamp_columns=("date_time",)
)

STAGING_DATASETS: tuple[StagingDataConfig, ...] = (
    COLLISIONS_STAGING_BASIC,
    COLLISIONS_STAGING_DETAILS,
)
