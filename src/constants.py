"""Common constants and enumerations."""

from enum import Enum


class FileFormat(Enum):
    """Supported file formats."""

    DELTA = "delta"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"
    ORC = "orc"


class WriteMode(Enum):
    """Spark write modes."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR_IF_EXISTS = "error"
    IGNORE = "ignore"


class LakeLayer(Enum):
    """Data lake medallion architecture layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    LANDING = "landing"
