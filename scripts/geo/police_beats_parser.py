"""Build a deterministic SDPD beat -> nearest NWS station mapping."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from pipeline.logging_config import configure_logging as configure_pipeline_logging, get_logger
from geopy.distance import geodesic
from shapely.geometry import Point, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_GEOJSON_PATH = PROJECT_ROOT / "pd_beats_datasd.geojson"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "beat_station_mapping.json"

WEATHER_STATIONS: list[dict[str, Any]] = [
    {
        "station_id": "KSAN",
        "name": "San Diego International Airport",
        "lat": 32.7338,
        "lon": -117.1933,
        "location": "Coastal/Downtown",
    },
    {
        "station_id": "KNZY",
        "name": "North Island Naval Air Station",
        "lat": 32.6992,
        "lon": -117.2153,
        "location": "Harbor/Coronado",
    },
    {
        "station_id": "KSDM",
        "name": "Brown Field Municipal",
        "lat": 32.5723,
        "lon": -116.9801,
        "location": "South San Diego",
    },
    {
        "station_id": "KMYF",
        "name": "Montgomery-Gibbs Executive",
        "lat": 32.8158,
        "lon": -117.1394,
        "location": "Kearny Mesa/Inland",
    },
    {
        "station_id": "KRNM",
        "name": "Ramona Airport",
        "lat": 33.04111,
        "lon": -116.91556,
        "location": "North Inland/East County",
    },
    {
        "station_id": "KSEE",
        "name": "Gillespie Field",
        "lat": 32.82472,
        "lon": -116.97222,
        "location": "East County/El Cajon",
    },
    {
        "station_id": "KNKX",
        "name": "MCAS Miramar",
        "lat": 32.86833,
        "lon": -117.14167,
        "location": "North Inland/Miramar",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Map SDPD beats to nearest weather stations."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_GEOJSON_PATH,
        help=f"Path to beat GeoJSON (default: {DEFAULT_GEOJSON_PATH})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Path to output mapping JSON (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Fail on source metadata conflicts inside duplicated beats "
            "(default: --strict)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    configure_pipeline_logging(
        level=level,
        service="scripts.geo.police_beats_parser",
    )


def validate_stations(stations: list[dict[str, Any]]) -> None:
    if not stations:
        raise RuntimeError("No weather stations configured.")

    seen: set[str] = set()
    for station in stations:
        station_id = station.get("station_id")
        if not isinstance(station_id, str) or not station_id:
            raise RuntimeError(f"Invalid station_id in station config: {station!r}")
        if station_id in seen:
            raise RuntimeError(f"Duplicate station_id in station config: {station_id}")
        seen.add(station_id)


def load_geojson(file_path: Path) -> dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("type") != "FeatureCollection":
        raise RuntimeError("Input GeoJSON must be a FeatureCollection.")
    features = data.get("features")
    if not isinstance(features, list):
        raise RuntimeError("Input GeoJSON features must be a list.")

    return data


def extract_beats(geojson_data: dict[str, Any]) -> list[dict[str, Any]]:
    beats: list[dict[str, Any]] = []

    for i, feature in enumerate(geojson_data["features"]):
        properties = feature.get("properties")
        geometry = feature.get("geometry")
        if not isinstance(properties, dict) or not isinstance(geometry, dict):
            raise RuntimeError(f"Feature index {i} missing properties or geometry.")

        required_props = ("objectid", "beat", "div", "serv")
        missing_props = [k for k in required_props if k not in properties]
        if missing_props:
            raise RuntimeError(
                f"Feature index {i} missing required properties: {missing_props}"
            )
        geometry_type = geometry.get("type")
        if geometry_type not in ("Polygon", "MultiPolygon"):
            raise RuntimeError(
                f"Feature index {i} has unsupported geometry type: {geometry_type!r}"
            )
        if "coordinates" not in geometry:
            raise RuntimeError(f"Feature index {i} missing geometry coordinates.")

        beats.append(
            {
                "objectid": int(properties["objectid"]),
                "beat": int(properties["beat"]),
                "div": int(properties["div"]) if properties["div"] is not None else None,
                "serv": int(properties["serv"]) if properties["serv"] is not None else None,
                "name": properties.get("name"),
                "geometry_type": geometry_type,
                "coordinates": geometry["coordinates"],
            }
        )

    return beats


def _single_value_or_raise(
    field_name: str, beat_id: int, values: set[Any], strict: bool
) -> Any:
    if len(values) == 1:
        return next(iter(values))
    message = f"Beat {beat_id} has conflicting {field_name} values: {sorted(values)!r}"
    if strict:
        raise RuntimeError(message)
    chosen = sorted(values, key=lambda value: str(value))[0]
    logger.warning("%s; using %r", message, chosen)
    return chosen


def _merge_beat_geometry(beat_id: int, geometries: list[BaseGeometry]) -> BaseGeometry:
    merged = unary_union(geometries)

    if merged.geom_type == "GeometryCollection":
        polygon_parts = [
            geom for geom in merged.geoms if geom.geom_type in ("Polygon", "MultiPolygon")
        ]
        if not polygon_parts:
            raise RuntimeError(
                f"Beat {beat_id} merged into GeometryCollection without polygonal parts."
            )
        merged = unary_union(polygon_parts)

    if merged.geom_type not in ("Polygon", "MultiPolygon"):
        raise RuntimeError(
            f"Beat {beat_id} merged to unsupported geometry type: {merged.geom_type}"
        )
    return merged


def consolidate_beats(beats: list[dict[str, Any]], strict: bool) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for beat in beats:
        grouped[beat["beat"]].append(beat)

    consolidated: list[dict[str, Any]] = []
    for beat_id, group in grouped.items():
        geometries = [
            shape({"type": row["geometry_type"], "coordinates": row["coordinates"]})
            for row in group
        ]
        merged_geom = _merge_beat_geometry(beat_id=beat_id, geometries=geometries)

        div = _single_value_or_raise(
            field_name="div",
            beat_id=beat_id,
            values={row["div"] for row in group},
            strict=strict,
        )
        serv = _single_value_or_raise(
            field_name="serv",
            beat_id=beat_id,
            values={row["serv"] for row in group},
            strict=strict,
        )

        non_null_names = sorted({row["name"] for row in group if row["name"] is not None})
        if len(non_null_names) > 1:
            message = f"Beat {beat_id} has conflicting non-null names: {non_null_names!r}"
            if strict:
                raise RuntimeError(message)
            logger.warning("%s; using %r", message, non_null_names[0])
        consolidated_name = non_null_names[0] if non_null_names else None

        source_objectids = sorted(row["objectid"] for row in group)
        source_null_name_count = sum(1 for row in group if row["name"] is None)
        consolidated.append(
            {
                "objectid": min(source_objectids),
                "source_objectids": source_objectids,
                "source_feature_count": len(group),
                "source_null_name_count": source_null_name_count,
                "beat": beat_id,
                "div": div,
                "serv": serv,
                "name": consolidated_name,
                "geometry": merged_geom,
                "geometry_type": merged_geom.geom_type,
            }
        )

    consolidated.sort(key=lambda row: row["beat"])
    return consolidated


def get_representative_point(geom: BaseGeometry) -> Point:
    if geom.geom_type == "MultiPolygon":
        geom = max(geom.geoms, key=lambda g: g.area)
    return geom.representative_point()


def find_nearest_station(point: Point, stations: list[dict[str, Any]]) -> dict[str, Any]:
    beat_coords = (point.y, point.x)
    nearest: dict[str, Any] | None = None
    min_distance = float("inf")

    for station in stations:
        station_coords = (station["lat"], station["lon"])
        distance_km = geodesic(beat_coords, station_coords).kilometers
        if distance_km < min_distance:
            min_distance = distance_km
            nearest = station.copy()
            nearest["distance_km"] = round(distance_km, 2)

    if nearest is None:
        raise RuntimeError("No weather station candidates were provided.")
    return nearest


def build_beat_station_mapping(
    geojson_path: Path, stations: list[dict[str, Any]], strict: bool
) -> list[dict[str, Any]]:
    validate_stations(stations)
    geojson_data = load_geojson(geojson_path)
    raw_beats = extract_beats(geojson_data)
    beats = consolidate_beats(raw_beats, strict=strict)
    mapping: list[dict[str, Any]] = []

    for beat in beats:
        rep_point = get_representative_point(beat["geometry"])
        nearest_station = find_nearest_station(point=rep_point, stations=stations)

        mapping.append(
            {
                "objectid": beat["objectid"],
                "source_objectids": beat["source_objectids"],
                "source_feature_count": beat["source_feature_count"],
                "source_null_name_count": beat["source_null_name_count"],
                "beat": beat["beat"],
                "div": beat["div"],
                "serv": beat["serv"],
                "name": beat["name"],
                "geometry_type": beat["geometry_type"],
                "representative_lat": round(rep_point.y, 6),
                "representative_lon": round(rep_point.x, 6),
                "station_id": nearest_station["station_id"],
                "station_name": nearest_station["name"],
                "station_location": nearest_station["location"],
                "distance_to_station_km": nearest_station["distance_km"],
            }
        )

    return mapping


def save_mapping(mapping: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2)
    logger.info("Saved beat-to-station mapping: %s", output_path)


def validate_mapping(mapping: list[dict[str, Any]]) -> None:
    if not mapping:
        raise RuntimeError("Generated mapping is empty.")

    beat_ids = [entry["beat"] for entry in mapping]
    duplicate_count = len(mapping) - len(set(beat_ids))
    if duplicate_count > 0:
        raise RuntimeError(f"Output contains duplicate beat IDs: {duplicate_count}")

    distances = [entry["distance_to_station_km"] for entry in mapping]
    station_counts: dict[str, int] = defaultdict(int)
    for entry in mapping:
        station_counts[entry["station_id"]] += 1

    source_null_name_total = sum(entry["source_null_name_count"] for entry in mapping)
    source_duplicates = sum(max(entry["source_feature_count"] - 1, 0) for entry in mapping)
    multipolygon_entries = [
        entry for entry in mapping if entry["geometry_type"] == "MultiPolygon"
    ]

    logger.info("Validation summary:")
    logger.info("  Total beats: %s", len(mapping))
    logger.info("  Duplicate beat IDs in output: %s", duplicate_count)
    logger.info("  Source duplicate feature count collapsed: %s", source_duplicates)
    logger.info("  Source null-name feature count: %s", source_null_name_total)
    logger.info("  Max distance: %.2f km", max(distances))
    logger.info("  Avg distance: %.2f km", sum(distances) / len(distances))
    logger.info("  MultiPolygon beats: %s", len(multipolygon_entries))
    logger.info("  Beats per station:")
    for station_id, count in sorted(station_counts.items()):
        logger.info("    %s: %s beats", station_id, count)


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    input_path = args.input.resolve()
    output_path = args.output.resolve()

    logger.info("Loading beat data from: %s", input_path)
    mapping = build_beat_station_mapping(
        geojson_path=input_path, stations=WEATHER_STATIONS, strict=args.strict
    )
    save_mapping(mapping=mapping, output_path=output_path)
    validate_mapping(mapping)
    logger.info("Mapping complete.")


if __name__ == "__main__":
    main()
