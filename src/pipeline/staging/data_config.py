from __future__ import annotations
from dataclasses import dataclass, field

@dataclass(frozen=True)
class StagingDataConfig:
    name: str
    table_name: str
    daily_file_name: str
    columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    integer_columns: tuple[str, ...] = ()
    timestamp_columns: tuple[str, ...] = ()
    generated_columns: tuple[str, ...] = ()
    column_renames: dict[str, str] = field(default_factory=dict)
    
COLLISIONS_STAGING_BASIC = StagingDataConfig(
    name="staging_collisions_basic",
    table_name="raw.collisions_basic",
    daily_file_name="pd_collisions_datasd.csv",
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
    table_name="raw.collisions_details",
    daily_file_name="pd_collisions_details_datasd.csv",
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
    timestamp_columns=("date_time",),
    generated_columns=("source_row_num",),
)

COLLISIONS_STAGING_BEATS = StagingDataConfig(
    name="staging_collisions_beats",
    table_name="raw.collisions_beats",
    daily_file_name="pd_beats_datasd.csv",
    columns = (
        "objectid",
        "beat",
        "div",
        "serv",
        "name"
    ),
    required_columns=(
        "objectid",
        "beat",
        "div",
        "serv",
    ),
    integer_columns=("beat",),
    column_renames={"objectid": "object_id"}
)

STAGING_DATASETS: tuple[StagingDataConfig, ...] = (
    COLLISIONS_STAGING_BASIC,
    COLLISIONS_STAGING_DETAILS,
    COLLISIONS_STAGING_BEATS
)
