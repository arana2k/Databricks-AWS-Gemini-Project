"""
schema_loader.py
----------------
Loads and validates schemas.json. Import this in generate_retail_data.py
(or pipelineclient.py) instead of keeping SCHEMAS as a hardcoded dict.
"""

import json
import os
from typing import Optional

REQUIRED_KEYS = {"columns", "id_prefix", "id_column", "description"}

# Default: schemas.json sits next to this file — works regardless of CWD
_DEFAULT_SCHEMAS_PATH = os.path.join(os.path.dirname(__file__), "schemas.json")


def load_schemas(path: str = _DEFAULT_SCHEMAS_PATH) -> dict:
    """
    Load schemas from a JSON file and validate structure.

    Args:
        path: Absolute or relative path to schemas.json.
              Defaults to schemas.json in the current working directory.

    Returns:
        dict keyed by entity name, e.g. SCHEMAS["online_orders"]

    Raises:
        FileNotFoundError: if the file doesn't exist at `path`.
        ValueError: if any entity is missing required keys.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"schemas.json not found at: {os.path.abspath(path)}")

    with open(path, "r", encoding="utf-8") as f:
        schemas = json.load(f)  # plain json.load — no special parsing needed

    # Validate each entity has required keys
    for entity, definition in schemas.items():
        missing = REQUIRED_KEYS - definition.keys()
        if missing:
            raise ValueError(f"Schema '{entity}' is missing required keys: {missing}")
        if not isinstance(definition["columns"], list):
            raise ValueError(f"Schema '{entity}': 'columns' must be a JSON array")

    return schemas


def get_schema(
    entity: str, schemas: Optional[dict] = None, path: str = _DEFAULT_SCHEMAS_PATH
) -> dict:
    """
    Convenience: get a single entity's schema dict.

    Usage:
        schema = get_schema("online_orders")
        schema = get_schema("online_orders", schemas=already_loaded_dict)
    """
    if schemas is None:
        schemas = load_schemas(path)
    if entity not in schemas:
        raise KeyError(
            f"Entity '{entity}' not found in schemas. Available: {list(schemas.keys())}"
        )
    return schemas[entity]
