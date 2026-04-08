"""
Utility functions for Parallel Web Systems plugin.

This module contains common utility functions used by both the connector and recipe.
"""

import logging

logger = logging.getLogger(__name__)


def map_type_to_python(type_name):
    """
    Map type string to Python type.

    Args:
        type_name: Type name string (string, int, float, bool)

    Returns:
        type: Corresponding Python type (str, int, float, bool)

    Raises:
        ValueError: If type_name is not supported
    """
    type_mapping = {
        "string": str,
        "int": int,
        "float": float,
        "bool": bool,
    }
    if type_name not in type_mapping:
        raise ValueError(f"Unsupported type: '{type_name}'. Supported types: {list(type_mapping.keys())}")
    return type_mapping[type_name]


def map_type_to_json_schema(type_name):
    """
    Map type string to JSON schema type.

    Args:
        type_name: Type name string (string, int, float, bool)

    Returns:
        str: JSON schema type (string, integer, number, boolean)
    """
    type_mapping = {
        "string": "string",
        "int": "integer",
        "float": "number",
        "bool": "boolean",
    }
    return type_mapping.get(type_name, "string")


def map_type_to_dataiku(type_name):
    """
    Map type string to Dataiku column type.

    Args:
        type_name: Type name string (string, int, float, bool)

    Returns:
        str: Dataiku column type (string, bigint, double, boolean)
    """
    type_mapping = {
        "string": "string",
        "int": "bigint",
        "float": "double",
        "bool": "boolean",
    }
    return type_mapping.get(type_name, "string")


def normalize_column_name(name):
    """
    Normalize column name: trim, lowercase, and replace spaces with underscores.

    Args:
        name: Column name string

    Returns:
        str: Normalized column name
    """
    return name.strip().lower().replace(' ', '_')


def parse_and_validate_fields(fields):
    """
    Parse and validate field configuration list.

    Each field must have:
    - name: Non-empty string
    - type: Valid type (string, int, float, bool)
    - description: Non-empty string

    Args:
        fields: List of field dicts with 'name', 'type', 'description'

    Returns:
        list: List of validated and normalized field definitions

    Raises:
        ValueError: If fields configuration is invalid
    """
    if not fields:
        logger.debug("No fields provided")
        return []

    if not isinstance(fields, list):
        raise ValueError("Fields must be a list")

    parsed = []
    seen = set()

    for idx, item in enumerate(fields):
        if not isinstance(item, dict):
            raise ValueError(f"Field {idx} is not a dict")

        name = (item.get("name") or "").strip()
        field_type = (item.get("type") or "").strip().lower()
        description = (item.get("description") or "").strip()

        if not name:
            raise ValueError(f"Field {idx} has no name")
        if not description:
            raise ValueError(f"Field '{name}' has no description")
        if name in seen:
            raise ValueError(f"Duplicate field name: '{name}'")

        # Validate type
        valid_types = ["string", "int", "float", "bool"]
        if field_type not in valid_types:
            raise ValueError(f"Field '{name}' has invalid type '{field_type}'. Valid types: {valid_types}")

        seen.add(name)

        parsed.append({
            "name": name,
            "type": field_type,
            "description": description,
        })
        logger.debug(f"Parsed field: {name} ({field_type})")

    logger.info(f"Parsed {len(parsed)} fields")
    return parsed


def convert_to_dict(value):
    """
    Convert API response objects to plain Python dict.

    Handles Pydantic models and other objects with dict conversion methods.

    Args:
        value: API response value (object, dict, or primitive)

    Returns:
        Plain Python type (dict, list, str, int, float, bool, or original value)
    """
    if value is None:
        return None

    if isinstance(value, (dict, list, str, int, float, bool)):
        return value

    # Try Pydantic v2 model_dump
    if hasattr(value, "model_dump"):
        return value.model_dump()

    # Try Pydantic v1 dict
    if hasattr(value, "dict"):
        return value.dict()

    # Generic __dict__ fallback
    if hasattr(value, "__dict__"):
        return value.__dict__

    return value
