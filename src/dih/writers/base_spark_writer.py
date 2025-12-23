"""Spark DataFrame writer implementation."""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

from src.dih.core.table_interfaces import TableDefinition, TargetTableDefMixin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def _ensure_schema_exists(table_name: str) -> None:
    """Create schema if it doesn't exist for managed tables."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        return

    parts = table_name.split(".")
    if len(parts) >= 2:
        # Extract schema name (second to last part)
        schema = parts[-2] if len(parts) == 2 else f"{parts[0]}.{parts[1]}"
        logger.debug(f"Ensuring schema exists: {schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logger.debug(f"Schema {schema} ready")



class AbstractWriter(ABC):
    """Abstract writer interface."""

    @abstractmethod
    def write(self, df: DataFrame, output_def: TableDefinition, **kwargs: Any) -> None:
        """Write DataFrame to target."""
        ...


class SparkDataFrameWriter(AbstractWriter):
    """Generic Spark DataFrame writer."""

    def write(self, df: DataFrame, output_def: TableDefinition, **kwargs: Any) -> None:
        """Write DataFrame to target defined in TableDefinition.

        Supports both managed (catalog) and unmanaged (path-based) tables.
        For managed tables, uses saveAsTable() with the table_name.
        For unmanaged tables, uses save() with the path.
        """
        file_format = output_def.format
        options = output_def.options

        if not file_format:
            msg = "Format must be specified in TableDefinition"
            raise ValueError(msg)

        writer = df.write.format(file_format)

        if options:
            writer = writer.options(**options)

        # Check for target-specific properties
        if isinstance(output_def, TargetTableDefMixin):
            if partition_by := output_def.partition_by:
                writer = writer.partitionBy(*partition_by)

            if write_mode := output_def.write_mode:
                writer = writer.mode(write_mode)

            # Handle managed vs unmanaged tables
            if output_def.managed:
                table_name = output_def.table_name
                if not table_name:
                    msg = "table_name must be specified when managed=True"
                    raise ValueError(msg)
                _ensure_schema_exists(table_name)
                logger.info(f"Writing managed {file_format} table: {table_name}")
                writer.saveAsTable(table_name)
                logger.info(f"Successfully wrote managed table: {table_name}")
            else:
                path = output_def.path
                if not path:
                    msg = "Path must be specified for unmanaged tables"
                    raise ValueError(msg)
                logger.info(f"Writing unmanaged {file_format} to {path}")
                writer.save(path)
                logger.info(f"Successfully wrote to {path}")
        else:
            # Fallback for non-TargetTableDefMixin definitions
            path = output_def.path
            if not path:
                msg = "Path must be specified in TableDefinition"
                raise ValueError(msg)
            logger.info(f"Writing {file_format} to {path}")
            writer.save(path)
            logger.info(f"Successfully wrote to {path}")
