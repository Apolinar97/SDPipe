"""Validate NWS Pydantic models against real 7-station observation JSON.

Parses each observation through NwsStationObservation, flattens to target
columns, and reports which columns are null across all stations.
"""

from __future__ import annotations

import json

import pytest

from pipeline.weather.models import NwsStationObservation
from pipeline.weather.nws_observation_flattener import flatten_observation

# ---------------------------------------------------------------------------
# 7-station sample JSON captured 2026-03-24
# ---------------------------------------------------------------------------
SAMPLE_BATCH_JSON = json.loads(r"""
{
  "captured_at_utc": "2026-03-25T00:05:59.047306+00:00",
  "stations_requested": ["KMYF", "KNKX", "KNZY", "KRNM", "KSAN", "KSDM", "KSEE"],
  "stations_failed": [],
  "source": "https://api.weather.gov",
  "schema_version": 1,
  "observations": [
    {
      "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld"],
      "id": "https://api.weather.gov/stations/KSDM/observations/2026-03-24T23:50:00+00:00",
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-116.98, 32.57]},
      "properties": {
        "@id": "https://api.weather.gov/stations/KSDM/observations/2026-03-24T23:50:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {"unitCode": "wmoUnit:m", "value": 160},
        "station": "https://api.weather.gov/stations/KSDM",
        "stationId": "KSDM",
        "stationName": "San Diego, Brown Field Municipal Airport",
        "timestamp": "2026-03-24T23:50:00+00:00",
        "rawMessage": "",
        "textDescription": "Clear",
        "icon": "https://api.weather.gov/icons/land/day/skc?size=medium",
        "presentWeather": [],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 21, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 14, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 290, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 12.96, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101490.06, "qualityControl": "V"},
        "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": null, "qualityControl": "Z"},
        "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44, "qualityControl": "C"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 64.287246008636, "qualityControl": "V"},
        "windChill": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 3810}, "amount": "CLR"}]
      }
    },
    {
      "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld"],
      "id": "https://api.weather.gov/stations/KRNM/observations/2026-03-24T23:50:00+00:00",
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-116.92, 33.03]},
      "properties": {
        "@id": "https://api.weather.gov/stations/KRNM/observations/2026-03-24T23:50:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {"unitCode": "wmoUnit:m", "value": 424},
        "station": "https://api.weather.gov/stations/KRNM",
        "stationId": "KRNM",
        "stationName": "Ramona, Ramona Airport",
        "timestamp": "2026-03-24T23:50:00+00:00",
        "rawMessage": "",
        "textDescription": "Clear",
        "icon": "https://api.weather.gov/icons/land/day/skc?size=medium",
        "presentWeather": [],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 27, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 14, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 260, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 12.96, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101490.06, "qualityControl": "V"},
        "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": null, "qualityControl": "Z"},
        "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44, "qualityControl": "C"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 44.830942359632, "qualityControl": "V"},
        "windChill": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": 27.11697451170611, "qualityControl": "V"},
        "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 3810}, "amount": "CLR"}]
      }
    },
    {
      "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld"],
      "id": "https://api.weather.gov/stations/KSEE/observations/2026-03-24T22:47:00+00:00",
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-116.97, 32.83]},
      "properties": {
        "@id": "https://api.weather.gov/stations/KSEE/observations/2026-03-24T22:47:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {"unitCode": "wmoUnit:m", "value": 117},
        "station": "https://api.weather.gov/stations/KSEE",
        "stationId": "KSEE",
        "stationName": "Gillespie Field Airport",
        "timestamp": "2026-03-24T22:47:00+00:00",
        "rawMessage": "KSEE 242247Z 25009KT 10SM SKC 27/15 A2996",
        "textDescription": "Clear",
        "icon": "https://api.weather.gov/icons/land/day/skc?size=medium",
        "presentWeather": [],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 27, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 15, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 250, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 16.56, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101460, "qualityControl": "V"},
        "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": null, "qualityControl": "Z"},
        "visibility": {"unitCode": "wmoUnit:m", "value": 16090, "qualityControl": "C"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "precipitationLastHour": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "precipitationLast6Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 47.8218054153, "qualityControl": "V"},
        "windChill": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": 27.28700176991778, "qualityControl": "V"},
        "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": null}, "amount": "CLR"}]
      }
    },
    {
      "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld"],
      "id": "https://api.weather.gov/stations/KSAN/observations/2026-03-24T23:50:00+00:00",
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-117.17, 32.73]},
      "properties": {
        "@id": "https://api.weather.gov/stations/KSAN/observations/2026-03-24T23:50:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {"unitCode": "wmoUnit:m", "value": 9},
        "station": "https://api.weather.gov/stations/KSAN",
        "stationId": "KSAN",
        "stationName": "San Diego International Airport",
        "timestamp": "2026-03-24T23:50:00+00:00",
        "rawMessage": "",
        "textDescription": "Mostly Clear",
        "icon": "https://api.weather.gov/icons/land/day/few?size=medium",
        "presentWeather": [],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 21, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 15, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 270, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 12.96, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101557.8, "qualityControl": "V"},
        "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": null, "qualityControl": "Z"},
        "visibility": {"unitCode": "wmoUnit:m", "value": 14484.1, "qualityControl": "C"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 68.576121925976, "qualityControl": "V"},
        "windChill": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 609.6}, "amount": "FEW"}]
      }
    },
    {
      "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld"],
      "id": "https://api.weather.gov/stations/KNZY/observations/2026-03-24T22:52:00+00:00",
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-117.22, 32.7]},
      "properties": {
        "@id": "https://api.weather.gov/stations/KNZY/observations/2026-03-24T22:52:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {"unitCode": "wmoUnit:m", "value": 8},
        "station": "https://api.weather.gov/stations/KNZY",
        "stationId": "KNZY",
        "stationName": "San Diego, North Island, Naval Air Station",
        "timestamp": "2026-03-24T22:52:00+00:00",
        "rawMessage": "KNZY 242252Z 23006KT 10SM FEW015 FEW250 20/14 A2997 RMK AO2 SLP149 T02000144",
        "textDescription": "Mostly Clear",
        "icon": "https://api.weather.gov/icons/land/day/few?size=medium",
        "presentWeather": [],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 20, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 14.4, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 230, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 11.16, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101490, "qualityControl": "V"},
        "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": 101490, "qualityControl": "V"},
        "visibility": {"unitCode": "wmoUnit:m", "value": 16090, "qualityControl": "C"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "precipitationLastHour": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "precipitationLast6Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 70.170683098134, "qualityControl": "V"},
        "windChill": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "cloudLayers": [
                               {"base": {
                               "unitCode": "wmoUnit:m",
                               "value": 460
                               },
                                "amount": "FEW"
                               },
                                {"base": {"unitCode": "wmoUnit:m", "value": 7620}, "amount": "FEW"}]
      }
    },
    {
      "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld"],
      "id": "https://api.weather.gov/stations/KMYF/observations/2026-03-24T23:50:00+00:00",
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-117.13, 32.82]},
      "properties": {
        "@id": "https://api.weather.gov/stations/KMYF/observations/2026-03-24T23:50:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {"unitCode": "wmoUnit:m", "value": 129},
        "station": "https://api.weather.gov/stations/KMYF",
        "stationId": "KMYF",
        "stationName": "San Diego, Montgomery Field",
        "timestamp": "2026-03-24T23:50:00+00:00",
        "rawMessage": "",
        "textDescription": "Clear",
        "icon": "https://api.weather.gov/icons/land/day/skc?size=medium",
        "presentWeather": [],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 21, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 14, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 230, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 14.832, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101490.06, "qualityControl": "V"},
        "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": null, "qualityControl": "Z"},
        "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44, "qualityControl": "C"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 64.287246008636, "qualityControl": "V"},
        "windChill": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 3810}, "amount": "CLR"}]
      }
    },
    {
      "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld"],
      "id": "https://api.weather.gov/stations/KNKX/observations/2026-03-24T22:55:00+00:00",
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-117.12, 32.85]},
      "properties": {
        "@id": "https://api.weather.gov/stations/KNKX/observations/2026-03-24T22:55:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {"unitCode": "wmoUnit:m", "value": 128},
        "station": "https://api.weather.gov/stations/KNKX",
        "stationId": "KNKX",
        "stationName": "San Diego, Miramar MCAS/Mitscher Field Airport",
        "timestamp": "2026-03-24T22:55:00+00:00",
        "rawMessage": "KNKX 242255Z 28006KT 7SM CLR 23/14 A2997 RMK AO2 SLP143 T02330144 $",
        "textDescription": "Clear",
        "icon": "https://api.weather.gov/icons/land/day/skc?size=medium",
        "presentWeather": [],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 23.3, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 14.4, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 280, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 11.16, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101490, "qualityControl": "V"},
        "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": 101430, "qualityControl": "V"},
        "visibility": {"unitCode": "wmoUnit:m", "value": 11270, "qualityControl": "C"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": null},
        "precipitationLastHour": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "precipitationLast6Hours": {"unitCode": "wmoUnit:mm", "value": null, "qualityControl": "Z"},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 57.349137225921, "qualityControl": "V"},
        "windChill": {"unitCode": "wmoUnit:degC", "value": null, "qualityControl": "V"},
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": 23.18300524978778, "qualityControl": "V"},
        "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": null}, "amount": "CLR"}]
      }
    }
  ]
}
""")

TARGET_COLUMNS = [
    "station_id",
    "station_name",
    "observation_timestamp",
    "latitude",
    "longitude",
    "text_description",
    "temperature_c",
    "wind_direction_deg",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "visibility_m",
    "precip_last_1h_mm",
    "precip_last_3h_mm",
    "precip_last_6h_mm",
    "cloud_cover",
    "cloud_base_m",
    "captured_at_utc",
]

SAMPLE_CAPTURED_AT = "2026-03-25T00:05:59.047306+00:00"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNwsModelValidation:
    """Validate that Pydantic models parse real NWS JSON and flatten correctly."""

    def test_all_observations_parse(self) -> None:
        """All 7 station observations should parse without error."""
        raw_observations = SAMPLE_BATCH_JSON["observations"]
        assert len(raw_observations) == 7

        parsed = []
        for raw in raw_observations:
            obs = NwsStationObservation.model_validate(raw)
            parsed.append(obs)

        assert len(parsed) == 7
        station_ids = sorted(obs.properties.station_id for obs in parsed)
        assert station_ids == ["KMYF", "KNKX", "KNZY", "KRNM", "KSAN", "KSDM", "KSEE"]

    def test_flatten_all_observations(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Flatten all 7 observations and print results for inspection."""
        raw_observations = SAMPLE_BATCH_JSON["observations"]
        flattened_rows = []

        for raw in raw_observations:
            obs = NwsStationObservation.model_validate(raw)
            row = flatten_observation(obs, SAMPLE_CAPTURED_AT)
            flattened_rows.append(row)

        assert len(flattened_rows) == 7

        # Print flattened output for visual inspection
        print("\n" + "=" * 70)
        print("FLATTENED OBSERVATIONS")
        print("=" * 70)
        for row in flattened_rows:
            print(f"\n--- {row['station_id']} ({row['station_name']}) ---")
            for col in TARGET_COLUMNS:
                print(f"  {col:.<30} {row[col]}")

        # Identify columns that are null across ALL 7 stations
        all_null_cols = []
        for col in TARGET_COLUMNS:
            if all(row[col] is None for row in flattened_rows):
                all_null_cols.append(col)

        print("\n" + "=" * 70)
        print(f"COLUMNS NULL ACROSS ALL 7 STATIONS ({len(all_null_cols)}):")
        for col in all_null_cols:
            print(f"  - {col}")
        print("=" * 70)

    def test_coordinate_order(self) -> None:
        """Verify GeoJSON coordinate order: [lon, lat]."""
        raw = SAMPLE_BATCH_JSON["observations"][0]  # KSDM
        obs = NwsStationObservation.model_validate(raw)
        row = flatten_observation(obs, SAMPLE_CAPTURED_AT)

        # KSDM is in San Diego: lat ~32.57, lon ~-116.98
        assert row["latitude"] == pytest.approx(32.57, abs=0.01)
        assert row["longitude"] == pytest.approx(-116.98, abs=0.01)

    def test_qc_z_nulled(self) -> None:
        """Values with qualityControl='Z' should be nulled by safe_value."""
        raw = SAMPLE_BATCH_JSON["observations"][0]  # KSDM
        obs = NwsStationObservation.model_validate(raw)
        row = flatten_observation(obs, SAMPLE_CAPTURED_AT)

        # wind_gust has QC="Z" for KSDM
        assert row["wind_gust_kmh"] is None
        # temperature has QC="V" — should have a value
        assert row["temperature_c"] == 21

    def test_missing_precip_fields_parse_as_none(self) -> None:
        """Stations without precipitation fields should parse as None."""
        # KSDM has no precipitationLastHour or precipitationLast6Hours keys
        raw = SAMPLE_BATCH_JSON["observations"][0]  # KSDM
        obs = NwsStationObservation.model_validate(raw)

        assert obs.properties.precipitation_last_hour is None
        assert obs.properties.precipitation_last_6_hours is None

        row = flatten_observation(obs, SAMPLE_CAPTURED_AT)
        assert row["precip_last_1h_mm"] is None
        assert row["precip_last_6h_mm"] is None

    def test_extra_fields_ignored(self) -> None:
        """Fields not in the model (@context, dewpoint, etc.) are silently ignored."""
        raw = SAMPLE_BATCH_JSON["observations"][0]
        # Should not raise despite @context, dewpoint, barometricPressure, etc.
        obs = NwsStationObservation.model_validate(raw)
        assert obs.properties.station_id == "KSDM"

    def test_multi_cloud_layers(self) -> None:
        """Only the first cloud layer is captured; verify multi-layer station."""
        # KNZY has 2 cloud layers: FEW@460m, FEW@7620m
        knzy_raw = next(r for r in SAMPLE_BATCH_JSON["observations"] if r["properties"]["stationId"] == "KNZY")
        obs = NwsStationObservation.model_validate(knzy_raw)
        assert len(obs.properties.cloud_layers) == 2

        row = flatten_observation(obs, SAMPLE_CAPTURED_AT)
        # Flattener takes first layer only
        assert row["cloud_cover"] == "FEW"
        assert row["cloud_base_m"] == 460
