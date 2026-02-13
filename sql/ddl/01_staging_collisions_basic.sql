CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE
    IF NOT EXISTS staging.collisions_basic (
        report_id TEXT NOT NULL,
        date_time TIMESTAMP NOT NULL,
        police_beat integer NOT NULL,
        address_no_primary TEXT NOT NULL,
        address_pd_primary TEXT NULL,
        address_road_primary TEXT NULL,
        address_sfx_primary TEXT,
        address_pd_intersecting TEXT NULL,
        address_name_intersecting TEXT NULL,
        address_sfx_intersecting TEXT NULL,
        violation_section TEXT NULL,
        violation_type TEXT NULL,
        charge_desc TEXT NULL,
        injured integer NULL,
        killed integer NULL,
        hit_run_lvl TEXT NULL,
        snapshot_dt DATE NOT NULL,
        source_file TEXT NOT NULL,
        load_ts TIMESTAMPTZ NOT NULL DEFAULT now ()
    );

CREATE INDEX IF NOT EXISTS ix_stg_basic_report_id ON staging.collisions_basic (report_id);

CREATE INDEX IF NOT EXISTS ix_stg_basic_snapshot_dt ON staging.collisions_basic (snapshot_dt);

CREATE INDEX IF NOT EXISTS ix_stg_basic_report_snapshot ON staging.collisions_basic (report_id, snapshot_dt);