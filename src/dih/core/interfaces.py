"""Core interface definitions for DIH framework."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class TableDefinition(ABC):
    """Abstract base class for table definitions (source and target)."""

    root_path: str | Path | None = None

    @property
    @abstractmethod
    def path(self) -> str | None:
        """Return the path to the table/dataset."""
        ...

    @property
    def options(self) -> dict[str, Any]:
        """Return read/write options for the table."""
        return {}

    @property
    @abstractmethod
    def format(self) -> str | None:
        """Return the file format (delta, parquet, csv, etc.)."""
        ...

    @property
    def schema(self) -> StructType | None:
        """Return the schema definition if applicable."""
        return None

    @property
    def default_alias(self) -> str:
        """Return default alias based on class name."""
        return self.__class__.__name__


class TargetTableDefMixin(ABC):
    """Mixin to extend TableDefinition for write operations."""

    @property
    def partition_by(self) -> list[str] | None:
        """Return columns to partition by."""
        return None

    @property
    def write_mode(self) -> str | None:
        """Return write mode (append, overwrite, etc.)."""
        return None

    @property
    def primary_keys(self) -> list[str] | None:
        """Return primary/business key columns for merge operations."""
        return None

    @property
    def merge_options(self) -> dict[str, Any] | None:
        """
        Return Delta merge-specific options.

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
        return None


@dataclass
class IOInterface:
    """Generic I/O interface for readers and writers."""

    path: str = field(default="")
    options: dict[str, Any] = field(default_factory=dict)
    file_format: str = field(default="delta")
    schema: StructType | None = field(default=None)
    partition_by: list[str] | None = field(default=None)
    primary_keys: list[str] | None = field(default=None)

    @property
    def default_alias(self) -> str:
        """Return default alias based on class name."""
        return self.__class__.__name__


class IReader(ABC):
    """Abstract reader interface."""

    @abstractmethod
    def read(self, input_def: TableDefinition) -> DataFrame:
        """Read data from source and return DataFrame."""
        ...

    @property
    @abstractmethod
    def data(self) -> DataFrame:
        """Return the loaded DataFrame."""
        ...


class IWriter(ABC):
    """Abstract writer interface."""

    @abstractmethod
    def write(self, df: DataFrame, output_def: TableDefinition, **kwargs: Any) -> None:
        """Write DataFrame to target."""
        ...
