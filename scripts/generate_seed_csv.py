from __future__ import annotations

import csv
import json
from pathlib import Path

from pipeline.logging_config import configure_logging, get_logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
JSON_PATH = PROJECT_ROOT / "beat_station_mapping_V1.json"
CSV_PATH = PROJECT_ROOT / "sdpipe_transforms" / "seeds" / "beat_station_mapping.csv"

SEED_COLUMNS = ["beat", "station_id", "distance_to_station_km"]

logger = get_logger(__name__)


def main() -> None:
    configure_logging(service="scripts.generate_seed_csv")

    with JSON_PATH.open(encoding="utf-8") as f:
        mappings = json.load(f)

    logger.info("Loaded %d entries from %s", len(mappings), JSON_PATH.name)

    rows = sorted(
        (
            {
                "beat": entry["beat"],
                "station_id": entry["station_id"],
                "distance_to_station_km": entry["distance_to_station_km"],
            }
            for entry in mappings
        ),
        key=lambda r: r["beat"],
    )

    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SEED_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %d rows to %s", len(rows), CSV_PATH.relative_to(PROJECT_ROOT))


if __name__ == "__main__":
    main()
