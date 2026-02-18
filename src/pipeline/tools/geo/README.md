# SDPD Beat-to-Station Mapping Utility

This utility builds a deterministic mapping from SDPD beats to the nearest configured NWS station.

## File

- Script: `src/pipeline/tools/geo/police_beats_parser.py`
- Default input: `pd_beats_datasd.geojson`
- Default output: `beat_station_mapping.json`

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Run

From repo root:

```bash
python src/pipeline/tools/geo/police_beats_parser.py
```

## Common Options

```bash
python src/pipeline/tools/geo/police_beats_parser.py \
  --input pd_beats_datasd.geojson \
  --output beat_station_mapping.json \
  --strict \
  --log-level INFO
```

- `--strict` (default): fail if duplicate source features for the same beat have conflicting metadata.
- `--no-strict`: allow conflicts and choose a deterministic fallback value, with warnings.
- `--log-level`: `DEBUG|INFO|WARNING|ERROR`

## Output

The output JSON is one row per unique `beat` (no duplicate beat IDs), including:

- `beat`, `div`, `serv`, `name`
- `station_id`, `station_name`, `station_location`
- `distance_to_station_km`
- lineage fields: `source_objectids`, `source_feature_count`, `source_null_name_count`

## Validation Behavior

On each run, the script logs:

- total beats mapped
- duplicate beat count in output (must be 0)
- collapsed source duplicate count
- source null-name feature count
- max/avg distance
- beats per station

If output is empty or duplicate beat IDs remain, the script raises an error.
