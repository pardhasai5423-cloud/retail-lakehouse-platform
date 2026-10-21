"""Load Bronze CSV schemas from the project's YAML configuration."""

from pathlib import Path

import yaml
from pyspark.sql.types import StringType, StructField, StructType

from src.glue.batch_planner import COMPANIES, DATASETS


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SCHEMA_FILE = (
    PROJECT_ROOT / "config" / "schemas" / "source_schemas.yaml"
)

SUPPORTED_TYPES = frozenset({
    "string",
    "integer",
    "decimal",
})


def load_source_schema(company, dataset):
    """
    Return a Spark schema for one source dataset.

    Bronze retains all source columns as strings.
    """

    if company not in COMPANIES:
        raise ValueError(f"Unsupported company: {company!r}")

    if dataset not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset!r}")

    with SCHEMA_FILE.open("r", encoding="utf-8") as file:
        configuration = yaml.safe_load(file)

    if not isinstance(configuration, dict):
        raise ValueError("Invalid source schema configuration")

    columns = configuration.get(company, {}).get(dataset)

    if not isinstance(columns, dict) or not columns:
        raise ValueError(
            f"Missing schema for {company}.{dataset}"
        )

    fields = []

    for column_name, declared_type in columns.items():
        if (
            not isinstance(column_name, str)
            or not column_name.strip()
        ):
            raise ValueError("Invalid source column name")

        if declared_type not in SUPPORTED_TYPES:
            raise ValueError(
                f"Unsupported type for {column_name}: "
                f"{declared_type!r}"
            )

        fields.append(
            StructField(
                column_name,
                StringType(),
                nullable=True,
            )
        )

    return StructType(fields)
