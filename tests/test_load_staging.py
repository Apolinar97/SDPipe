"""Unit tests for the staging loader — preflight skip-missing and config consistency."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from pipeline.staging.data_config import (
    COLLISIONS_STAGING_BASIC,
    COLLISIONS_STAGING_BEATS,
    COLLISIONS_STAGING_DETAILS,
    NWS_OBSERVATIONS,
    STAGING_DATASETS,
    StagingDataConfig,
)
from pipeline.staging.load_staging import _preflight_resolve_sources
from pipeline.weather.nws_observation_flattener import CSV_COLUMNS

# ---------------------------------------------------------------------------
# Config consistency — catch drift between flattener, loader, and DDL
# ---------------------------------------------------------------------------


class TestNwsConfigConsistency:
    """Guard against the flattener, data_config, and DDL drifting apart."""

    def test_nws_registered_in_staging_datasets(self):
        names = [ds.name for ds in STAGING_DATASETS]
        assert "nws_observations" in names

    def test_flattener_columns_match_data_config(self):
        """The CSV the flattener writes must match what the loader expects."""
        assert tuple(CSV_COLUMNS) == NWS_OBSERVATIONS.columns

    def test_required_columns_subset_of_columns(self):
        cols = set(NWS_OBSERVATIONS.columns)
        for req in NWS_OBSERVATIONS.required_columns:
            assert req in cols, f"required column {req!r} not in columns"


# ---------------------------------------------------------------------------
# truncate_before_load flag — collision datasets truncate, NWS does not
# ---------------------------------------------------------------------------


class TestTruncateBeforeLoadFlag:
    """Ensure the truncate_before_load flag is set correctly per dataset."""

    def test_nws_observations_does_not_truncate(self):
        assert NWS_OBSERVATIONS.truncate_before_load is False

    def test_collision_basic_truncates(self):
        assert COLLISIONS_STAGING_BASIC.truncate_before_load is True

    def test_collision_details_truncates(self):
        assert COLLISIONS_STAGING_DETAILS.truncate_before_load is True

    def test_collision_beats_truncates(self):
        assert COLLISIONS_STAGING_BEATS.truncate_before_load is True

    def test_default_is_true(self):
        """New configs without explicit flag should default to truncate."""
        cfg = StagingDataConfig(
            name="test",
            table_name="raw.test",
            daily_file_name="test.csv",
            columns=("col1",),
            required_columns=("col1",),
        )
        assert cfg.truncate_before_load is True


# ---------------------------------------------------------------------------
# Preflight — skip-missing behaviour
# ---------------------------------------------------------------------------

RUN_DATE = date(2026, 3, 25)

# Two small configs for testing preflight in isolation
_DS_A = StagingDataConfig(
    name="ds_a",
    table_name="raw.ds_a",
    daily_file_name="a.csv",
    columns=("col1",),
    required_columns=("col1",),
)
_DS_B = StagingDataConfig(
    name="ds_b",
    table_name="raw.ds_b",
    daily_file_name="b.csv",
    columns=("col1",),
    required_columns=("col1",),
)
_TEST_DATASETS = (_DS_A, _DS_B)


def _mock_store(existing_keys: set[str]) -> MagicMock:
    store = MagicMock()
    store.bucket_name = "test-bucket"
    store.object_exists = lambda key: key in existing_keys
    return store


class TestPreflightSkipMissing:
    """Verify preflight skips missing datasets instead of failing."""

    @patch("pipeline.staging.load_staging.STAGING_DATASETS", _TEST_DATASETS)
    @patch.dict("os.environ", {"STAGING_SOURCE_ROOT": "raw"})
    def test_all_available(self):
        store = _mock_store({
            "raw/2026-03-25/a.csv",
            "raw/2026-03-25/b.csv",
        })
        resolved = _preflight_resolve_sources(RUN_DATE, store)
        assert set(resolved.keys()) == {"ds_a", "ds_b"}

    @patch("pipeline.staging.load_staging.STAGING_DATASETS", _TEST_DATASETS)
    @patch.dict("os.environ", {"STAGING_SOURCE_ROOT": "raw"})
    def test_one_missing_skips(self):
        store = _mock_store({"raw/2026-03-25/a.csv"})
        resolved = _preflight_resolve_sources(RUN_DATE, store)
        assert "ds_a" in resolved
        assert "ds_b" not in resolved

    @patch("pipeline.staging.load_staging.STAGING_DATASETS", _TEST_DATASETS)
    @patch.dict("os.environ", {"STAGING_SOURCE_ROOT": "raw"})
    def test_none_available_raises(self):
        store = _mock_store(set())
        with pytest.raises(RuntimeError, match="no source files available"):
            _preflight_resolve_sources(RUN_DATE, store)

    @patch("pipeline.staging.load_staging.STAGING_DATASETS", _TEST_DATASETS)
    @patch.dict("os.environ", {"STAGING_SOURCE_ROOT": "raw"})
    def test_resolved_keys_are_correct_paths(self):
        store = _mock_store({"raw/2026-03-25/a.csv"})
        resolved = _preflight_resolve_sources(RUN_DATE, store)
        assert resolved["ds_a"] == "raw/2026-03-25/a.csv"

    @patch("pipeline.staging.load_staging.STAGING_DATASETS", _TEST_DATASETS)
    @patch.dict("os.environ", {"STAGING_SOURCE_ROOT": "raw"})
    def test_backfill_different_date(self):
        """Backfill for an older date resolves the correct S3 keys."""
        backfill_date = date(2026, 3, 20)
        store = _mock_store({"raw/2026-03-20/b.csv"})
        resolved = _preflight_resolve_sources(backfill_date, store)
        assert "ds_b" in resolved
        assert resolved["ds_b"] == "raw/2026-03-20/b.csv"
        assert "ds_a" not in resolved
