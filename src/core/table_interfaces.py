"""Core interface definitions for ih framework."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from src.core.types import MergeOptionsDict


class VolumeNotFoundError(KeyError):
    """Raised when a requested volume is not found in the table definition."""

    def __init__(self, volume_name: str) -> None:
        self.volume_name = volume_name
        super().__init__(f"Volume '{volume_name}' not configured")


class TableDefinition(ABC):
    """Abstract base class for table definitions (source and target)."""

    catalog: str | None = None  # Injected at runtime from environment
    volumes: dict[str, str] | None = None  # Injected at runtime from environment

    def get_volume(self, name: str) -> str:
        """Get a volume path by name."""
        if self.volumes is None or name not in self.volumes:
            raise VolumeNotFoundError(name)
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
    def options(self) -> dict[str, str]:
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
    def merge_options(self) -> MergeOptionsDict | None:
        """Return Delta merge-specific options.

        See MergeOptionsDict for available options.
        """
        return None
