CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.nws_observations (
    station_id             TEXT NOT NULL,
    station_name           TEXT NOT NULL,
    observation_timestamp  TIMESTAMPTZ NOT NULL,
    latitude               DOUBLE PRECISION NOT NULL,
    longitude              DOUBLE PRECISION NOT NULL,
    text_description       TEXT NULL,
    temperature_c          DOUBLE PRECISION NULL,
    wind_direction_deg     DOUBLE PRECISION NULL,
    wind_speed_kmh         DOUBLE PRECISION NULL,
    wind_gust_kmh          DOUBLE PRECISION NULL,
    visibility_m           DOUBLE PRECISION NULL,
    precip_last_1h_mm      DOUBLE PRECISION NULL,
    precip_last_3h_mm      DOUBLE PRECISION NULL,
    precip_last_6h_mm      DOUBLE PRECISION NULL,
    cloud_cover            TEXT NULL,
    cloud_base_m           DOUBLE PRECISION NULL,
    captured_at_utc        TIMESTAMPTZ NOT NULL,
    snapshot_dt            DATE NOT NULL,
    source_file            TEXT NOT NULL,
    load_ts                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_nws_obs_station_id
    ON raw.nws_observations (station_id);

CREATE INDEX IF NOT EXISTS ix_nws_obs_observation_ts
    ON raw.nws_observations (observation_timestamp);

CREATE INDEX IF NOT EXISTS ix_nws_obs_station_obs_ts
    ON raw.nws_observations (station_id, observation_timestamp);
