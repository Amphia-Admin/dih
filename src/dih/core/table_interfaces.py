"""Core interface definitions for DIH framework."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class TableDefinition(ABC):
    """Abstract base class for table definitions (source and target)."""

    catalog: str | None = None  # Injected at runtime from environment
    volumes: dict[str, str] | None = None  # Injected at runtime from environment

    def get_volume(self, name: str) -> str:
        """Get a volume path by name."""
        if self.volumes is None or name not in self.volumes:
            raise KeyError(f"Volume '{name}' not configured")
        return self.volumes[name]

    @property
    @abstractmethod
    def path(self) -> str | None:
        """Return the path to the table/dataset."""
        ...

    @property
    def table_name(self) -> str | None:
        """Return the fully qualified table name for catalog tables.

        Format: 'catalog.schema.table' or 'schema.table'.
        Used by CatalogTableReader and DeltaTableReader for managed tables.
        """
        return None

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
    """Mixin to extend TableDefinition for write operations.

    Note: table_name is inherited from TableDefinition base class.
    Override it in your implementation to specify the target table name.
    """

    @property
    def managed(self) -> bool:
        """Return whether the table is managed (catalog) or unmanaged (path-based).

        If True, writes to the catalog using saveAsTable().
        If False, writes to the file system path using save().

        Default is False (unmanaged).
        """
        return False

    @property
    def partition_by(self) -> list[str] | None:
        """Return columns to partition by."""
        return None

    @property
    @abstractmethod
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


