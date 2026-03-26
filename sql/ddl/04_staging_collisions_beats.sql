CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.collisions_beats (
    object_id TEXT NOT NULL,
    beat INTEGER NOT NULL,
    div TEXT NOT NULL,
    serv TEXT NOT NULL,
    name TEXT NULL,
    snapshot_dt DATE NOT NULL,
    source_file TEXT NOT NULL,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);