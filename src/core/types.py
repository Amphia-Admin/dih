"""Type definitions for the ih framework.

This module provides type aliases and protocols.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypedDict, TypeVar

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import DataType

# Generic typing
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

# Spark type aliases
type SparkConfig = dict[str, str | int | bool]
type ColumnMapping = dict[str, str | Column]
type SchemaMapping = dict[str, DataType]

# Pipeline configuration types
type VolumeConfig = dict[str, str]
type PipelineMetadata = dict[str, str | int | bool | None]

# Reader/Writer configuration types
type ReaderOptions = dict[str, str | int | bool | None]
type WriterOptions = dict[str, str | int | bool | None]

# Kwargs value types for registry and writer methods
type WriteOptionsValue = str | int | bool | list[str] | None


class MergeOptionsDict(TypedDict, total=False):
    """
    Delta merge operation options.

    All fields are optional to allow partial configuration.

    Supported options:
        when_matched_update_condition: SQL condition for conditional update
        when_matched_delete_condition: SQL condition for conditional delete
        when_not_matched_insert_condition: SQL condition for conditional insert
        columns_to_update: List of columns to update (default: all non-PK columns)
        columns_to_insert: List of columns to insert (default: all columns)
        source_alias: Alias for source table (default: "src")
        target_alias: Alias for target table (default: "tgt")
        broadcast_threshold: Broadcast threshold in bytes (default: Spark default)
    """

    when_matched_update_condition: str | None
    when_matched_delete_condition: str | None
    when_not_matched_insert_condition: str | None
    columns_to_update: list[str] | None
    columns_to_insert: list[str] | None
    source_alias: str
    target_alias: str
    broadcast_threshold: int | None


class DeltaLogEntry(TypedDict):
    """Structured log entry for Delta table logging."""

    timestamp: datetime
    level: str
    logger: str
    message: str
    module: str
    function: str
    line: int
    thread: str | None
    exception: str | None


class LocalEnvConfigDict(TypedDict, total=False):
    """Local environment configuration from YAML."""

    catalog: str
    volumes: dict[str, str]


class RemoteEnvConfigDict(TypedDict, total=False):
    """Remote environment configuration from YAML."""

    catalog: str
    volumes: dict[str, str]
    secret_scope: str


class EnvironmentConfigDict(TypedDict, total=False):
    """Root environment configuration from YAML."""

    local: LocalEnvConfigDict
    remote: RemoteEnvConfigDict


class DataFrameProvider(Protocol):
    """Protocol for objects that provide DataFrames."""

    @property
    def data(self) -> DataFrame:
        """Return the DataFrame."""
        ...


class SparkSessionProvider(Protocol):
    """Protocol for objects that provide SparkSession."""

    def create_spark_session(self) -> SparkSession:
        """Create and return a SparkSession."""
        ...
