"""Unit tests for the NWS observation flattener module."""

from __future__ import annotations

import csv
import io
import json

import pytest

from pipeline.weather.nws_observation_flattener import (
    CSV_COLUMNS,
    _get_date_range,
    build_csv_bytes,
    deduplicate_observations,
    flatten_batch_file,
)

# Minimal batch JSON with 7 observations (same sample as model validation tests)
SAMPLE_BATCH_JSON = {
    "captured_at_utc": "2026-03-25T00:05:59.047306+00:00",
    "stations_requested": ["KMYF", "KNKX", "KNZY", "KRNM", "KSAN", "KSDM", "KSEE"],
    "stations_failed": [],
    "source": "https://api.weather.gov",
    "schema_version": 1,
    "observations": [
        {
            "id": "https://api.weather.gov/stations/KSDM/observations/2026-03-24T23:50:00+00:00",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-116.98, 32.57]},
            "properties": {
                "stationId": "KSDM",
                "stationName": "San Diego, Brown Field Municipal Airport",
                "timestamp": "2026-03-24T23:50:00+00:00",
                "textDescription": "Clear",
                "temperature": {"unitCode": "wmoUnit:degC", "value": 21, "qualityControl": "V"},
                "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 290, "qualityControl": "V"},
                "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 12.96, "qualityControl": "V"},
                "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
                "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44, "qualityControl": "C"},
                "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 3810}, "amount": "CLR"}],
            },
        },
        {
            "id": "https://api.weather.gov/stations/KRNM/observations/2026-03-24T23:50:00+00:00",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-116.92, 33.03]},
            "properties": {
                "stationId": "KRNM",
                "stationName": "Ramona, Ramona Airport",
                "timestamp": "2026-03-24T23:50:00+00:00",
                "textDescription": "Clear",
                "temperature": {"unitCode": "wmoUnit:degC", "value": 27, "qualityControl": "V"},
                "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 260, "qualityControl": "V"},
                "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 12.96, "qualityControl": "V"},
                "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
                "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44, "qualityControl": "C"},
                "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 3810}, "amount": "CLR"}],
            },
        },
        {
            "id": "https://api.weather.gov/stations/KSEE/observations/2026-03-24T22:47:00+00:00",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-116.97, 32.83]},
            "properties": {
                "stationId": "KSEE",
                "stationName": "Gillespie Field Airport",
                "timestamp": "2026-03-24T22:47:00+00:00",
                "textDescription": "Clear",
                "temperature": {"unitCode": "wmoUnit:degC", "value": 27, "qualityControl": "V"},
                "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 250, "qualityControl": "V"},
                "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 16.56, "qualityControl": "V"},
                "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
                "visibility": {"unitCode": "wmoUnit:m", "value": 16090, "qualityControl": "C"},
                "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": None}, "amount": "CLR"}],
            },
        },
        {
            "id": "https://api.weather.gov/stations/KSAN/observations/2026-03-24T23:50:00+00:00",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-117.17, 32.73]},
            "properties": {
                "stationId": "KSAN",
                "stationName": "San Diego International Airport",
                "timestamp": "2026-03-24T23:50:00+00:00",
                "textDescription": "Mostly Clear",
                "temperature": {"unitCode": "wmoUnit:degC", "value": 21, "qualityControl": "V"},
                "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 270, "qualityControl": "V"},
                "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 12.96, "qualityControl": "V"},
                "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
                "visibility": {"unitCode": "wmoUnit:m", "value": 14484.1, "qualityControl": "C"},
                "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 609.6}, "amount": "FEW"}],
            },
        },
        {
            "id": "https://api.weather.gov/stations/KNZY/observations/2026-03-24T22:52:00+00:00",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-117.22, 32.7]},
            "properties": {
                "stationId": "KNZY",
                "stationName": "San Diego, North Island, Naval Air Station",
                "timestamp": "2026-03-24T22:52:00+00:00",
                "textDescription": "Mostly Clear",
                "temperature": {"unitCode": "wmoUnit:degC", "value": 20, "qualityControl": "V"},
                "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 230, "qualityControl": "V"},
                "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 11.16, "qualityControl": "V"},
                "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
                "visibility": {"unitCode": "wmoUnit:m", "value": 16090, "qualityControl": "C"},
                "cloudLayers": [
                    {"base": {"unitCode": "wmoUnit:m", "value": 460}, "amount": "FEW"},
                    {"base": {"unitCode": "wmoUnit:m", "value": 7620}, "amount": "FEW"},
                ],
            },
        },
        {
            "id": "https://api.weather.gov/stations/KMYF/observations/2026-03-24T23:50:00+00:00",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-117.13, 32.82]},
            "properties": {
                "stationId": "KMYF",
                "stationName": "San Diego, Montgomery Field",
                "timestamp": "2026-03-24T23:50:00+00:00",
                "textDescription": "Clear",
                "temperature": {"unitCode": "wmoUnit:degC", "value": 21, "qualityControl": "V"},
                "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 230, "qualityControl": "V"},
                "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 14.832, "qualityControl": "V"},
                "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
                "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44, "qualityControl": "C"},
                "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 3810}, "amount": "CLR"}],
            },
        },
        {
            "id": "https://api.weather.gov/stations/KNKX/observations/2026-03-24T22:55:00+00:00",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-117.12, 32.85]},
            "properties": {
                "stationId": "KNKX",
                "stationName": "San Diego, Miramar MCAS/Mitscher Field Airport",
                "timestamp": "2026-03-24T22:55:00+00:00",
                "textDescription": "Clear",
                "temperature": {"unitCode": "wmoUnit:degC", "value": 23.3, "qualityControl": "V"},
                "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 280, "qualityControl": "V"},
                "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 11.16, "qualityControl": "V"},
                "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
                "visibility": {"unitCode": "wmoUnit:m", "value": 11270, "qualityControl": "C"},
                "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": None}, "amount": "CLR"}],
            },
        },
    ],
}


def _make_stream(data: dict) -> io.BytesIO:
    return io.BytesIO(json.dumps(data).encode("utf-8"))


class TestFlattenBatchFile:
    def test_flatten_batch_file(self) -> None:
        """Feed JSON bytes via BytesIO, assert 7 rows with correct keys."""
        rows = flatten_batch_file(_make_stream(SAMPLE_BATCH_JSON), "test/batch.json")
        assert len(rows) == 7
        for row in rows:
            assert set(row.keys()) == set(CSV_COLUMNS)
            assert row["captured_at_utc"] == "2026-03-25T00:05:59.047306+00:00"

    def test_empty_observations(self) -> None:
        """Batch with empty observations list returns empty list."""
        batch = {**SAMPLE_BATCH_JSON, "observations": []}
        rows = flatten_batch_file(_make_stream(batch), "test/empty.json")
        assert rows == []


class TestDeduplicateObservations:
    def test_deduplicate_keeps_all_hourly_readings(self) -> None:
        """Two rows same station, different timestamps — both are kept."""
        rows = [
            {
                "station_id": "KSAN",
                "observation_timestamp": "2026-03-24T22:00:00+00:00",
                "captured_at_utc": "2026-03-25T00:05:00+00:00",
                "temperature_c": 19,
            },
            {
                "station_id": "KSAN",
                "observation_timestamp": "2026-03-24T23:50:00+00:00",
                "captured_at_utc": "2026-03-25T00:05:00+00:00",
                "temperature_c": 21,
            },
        ]
        result = deduplicate_observations(rows)
        assert len(result) == 2
        assert result[0]["observation_timestamp"] == "2026-03-24T22:00:00+00:00"
        assert result[1]["observation_timestamp"] == "2026-03-24T23:50:00+00:00"

    def test_deduplicate_tiebreaker(self) -> None:
        """Same station + timestamp, different captured_at_utc — latest capture wins."""
        rows = [
            {
                "station_id": "KSAN",
                "observation_timestamp": "2026-03-24T23:50:00+00:00",
                "captured_at_utc": "2026-03-25T00:05:00+00:00",
                "temperature_c": 20,
            },
            {
                "station_id": "KSAN",
                "observation_timestamp": "2026-03-24T23:50:00+00:00",
                "captured_at_utc": "2026-03-25T01:10:00+00:00",
                "temperature_c": 21,
            },
        ]
        result = deduplicate_observations(rows)
        assert len(result) == 1
        assert result[0]["captured_at_utc"] == "2026-03-25T01:10:00+00:00"
        assert result[0]["temperature_c"] == 21


class TestBuildCsvBytes:
    def test_build_csv_bytes(self) -> None:
        """Verify output is valid CSV with correct header, row count, and quoting."""
        rows = flatten_batch_file(_make_stream(SAMPLE_BATCH_JSON), "test/batch.json")
        csv_bytes = build_csv_bytes(rows)

        reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8")))
        assert reader.fieldnames == CSV_COLUMNS

        csv_rows = list(reader)
        assert len(csv_rows) == 7

        # Station names with commas should be properly quoted
        ksdm = next(r for r in csv_rows if r["station_id"] == "KSDM")
        assert ksdm["station_name"] == "San Diego, Brown Field Municipal Airport"


class TestGetDateRange:
    def test_single_date(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Start only → end defaults to start, range is one day."""
        monkeypatch.setenv("NWS_FLATTEN_RUN_DATE", "2026-03-25")
        monkeypatch.delenv("NWS_FLATTEN_END_DATE", raising=False)
        start, end = _get_date_range()
        assert start == end
        assert str(start) == "2026-03-25"

    def test_date_range(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Explicit start and end returns correct range."""
        monkeypatch.setenv("NWS_FLATTEN_RUN_DATE", "2026-02-25")
        monkeypatch.setenv("NWS_FLATTEN_END_DATE", "2026-03-25")
        start, end = _get_date_range()
        assert str(start) == "2026-02-25"
        assert str(end) == "2026-03-25"
        assert (end - start).days == 28

    def test_end_before_start_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """End date before start date raises RuntimeError."""
        monkeypatch.setenv("NWS_FLATTEN_RUN_DATE", "2026-03-25")
        monkeypatch.setenv("NWS_FLATTEN_END_DATE", "2026-03-20")
        with pytest.raises(RuntimeError, match="NWS_FLATTEN_END_DATE"):
            _get_date_range()

    def test_invalid_date_format_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid date string raises RuntimeError."""
        monkeypatch.setenv("NWS_FLATTEN_RUN_DATE", "25-03-2026")
        monkeypatch.delenv("NWS_FLATTEN_END_DATE", raising=False)
        with pytest.raises(RuntimeError, match="NWS_FLATTEN_RUN_DATE"):
            _get_date_range()
