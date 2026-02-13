CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE
    IF NOT EXISTS staging.collisions_details (
        report_id TEXT NOT NULL,
        date_time TIMESTAMP NOT NULL,
        person_role TEXT NULL,
        person_injury_lvl TEXT NULL,
        person_veh_type TEXT NULL,
        veh_type TEXT NULL,
        veh_make TEXT NULL,
        veh_model TEXT NULL,
        police_beat INTEGER NOT NULL,
        address_no_primary TEXT NULL,
        address_pd_primary TEXT NULL,
        address_road_primary TEXT NULL,
        address_sfx_primary TEXT NULL,
        address_pd_intersecting TEXT NULL,
        address_name_intersecting TEXT NULL,
        address_sfx_intersecting TEXT NULL,
        violation_section TEXT NULL,
        violation_type TEXT NULL,
        charge_desc TEXT NULL,
        injured INTEGER NULL,
        killed INTEGER NULL,
        hit_run_lvl TEXT NULL,
        snapshot_dt DATE NOT NULL,
        source_file TEXT NOT NULL,
        load_ts TIMESTAMPTZ NOT NULL DEFAULT now ()
    );

CREATE INDEX IF NOT EXISTS ix_stg_details_report_id ON staging.collisions_details (report_id);

CREATE INDEX IF NOT EXISTS ix_stg_details_snapshot_dt ON staging.collisions_details (snapshot_dt);

CREATE INDEX IF NOT EXISTS ix_stg_details_report_snapshot ON staging.collisions_details (report_id, snapshot_dt);