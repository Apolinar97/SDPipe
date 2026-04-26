#!/usr/bin/env python3
"""Generate ai_context.json from dbt manifest and catalog artifacts."""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
MANIFEST_PATH = REPO_ROOT / "sdpipe_transforms" / "target" / "manifest.json"
CATALOG_PATH = REPO_ROOT / "sdpipe_transforms" / "target" / "catalog.json"
OUTPUT_PATH = REPO_ROOT / "ai_context.json"

TYPE_ORDER = {"fact": 0, "dimension": 1, "bridge": 2}


def _is_marts_model(node: dict) -> bool:
    # fqn looks like ["sdpipe_transforms", "marts", "fct_collisions"]
    fqn = node.get("fqn", [])
    return len(fqn) >= 2 and fqn[-2] == "marts"


def _model_type(name: str) -> str:
    if name.startswith("fct_"):
        return "fact"
    if name.startswith("dim_"):
        return "dimension"
    if name.startswith("map_"):
        return "bridge"
    return "other"


def _attached_model_name(node: dict, marts_model_names: set[str]) -> str | None:
    name = node.get("attached_node", "").split(".")[-1]
    return name if name in marts_model_names else None


def build_context() -> None:
    if not MANIFEST_PATH.exists():
        print(f"Error: manifest not found at {MANIFEST_PATH}", file=sys.stderr)
        print("Run: cd sdpipe_transforms && dbt docs generate", file=sys.stderr)
        sys.exit(1)

    manifest = json.loads(MANIFEST_PATH.read_text())

    catalog_nodes: dict = {}
    if CATALOG_PATH.exists():
        catalog = json.loads(CATALOG_PATH.read_text())
        catalog_nodes = catalog.get("nodes", {})
    else:
        print("Warning: catalog.json not found — column types will fall back to schema.yml values", file=sys.stderr)

    # Discover marts model names dynamically from fqn
    marts_model_names = {
        node["name"]
        for node in manifest["nodes"].values()
        if node.get("resource_type") == "model" and _is_marts_model(node)
    }

    # Collect FK relationships and not_null flags from test nodes
    relationships: dict[str, list] = {m: [] for m in marts_model_names}
    non_nullable: set[tuple[str, str]] = set()

    for node in manifest["nodes"].values():
        if node.get("resource_type") != "test":
            continue
        meta = node.get("test_metadata", {})
        test_name = meta.get("name", "")
        model_name = _attached_model_name(node, marts_model_names)
        if not model_name:
            continue
        col = node.get("column_name", "")

        if test_name == "not_null":
            non_nullable.add((model_name, col.lower()))
        elif test_name == "relationships":
            kwargs = meta.get("kwargs", {})
            to_ref = kwargs.get("to", "")
            to_col = kwargs.get("field", "")
            match = re.search(r"ref\(['\"](\w+)['\"]\)", to_ref)
            to_model = match.group(1) if match else to_ref
            relationships[model_name].append({"from_column": col, "to_model": to_model, "to_column": to_col})

    models = []
    for key, node in manifest["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        model_name = node["name"]
        if model_name not in marts_model_names:
            continue

        catalog_cols = {k.lower(): v.get("type", "") for k, v in catalog_nodes.get(key, {}).get("columns", {}).items()}

        columns = []
        for col_name, manifest_col in node.get("columns", {}).items():
            col_lower = col_name.lower()
            columns.append(
                {
                    "name": col_name,
                    "data_type": catalog_cols.get(col_lower) or manifest_col.get("data_type") or "unknown",
                    "description": manifest_col.get("description", ""),
                    "nullable": (model_name, col_lower) not in non_nullable,
                }
            )

        models.append(
            {
                "name": model_name,
                "type": _model_type(model_name),
                "materialization": node.get("config", {}).get("materialized", ""),
                "description": node.get("description", ""),
                "columns": columns,
                "relationships": relationships[model_name],
            }
        )

    models.sort(key=lambda m: (TYPE_ORDER.get(m["type"], 99), m["name"]))

    context = {
        "project": "SDPipe — San Diego Traffic Collision & Weather Data",
        "database": "sdpwarehouse",
        "schema": "marts",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "models": models,
    }

    OUTPUT_PATH.write_text(json.dumps(context, indent=2))
    print(f"Wrote {OUTPUT_PATH} ({len(models)} models)")


if __name__ == "__main__":
    build_context()
