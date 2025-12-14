"""Spark DataFrame writer implementation."""

import logging
from typing import TYPE_CHECKING, Any

from dih.core.interfaces import IWriter, TableDefinition, TargetTableDefMixin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class SparkDataFrameWriter(IWriter):
    """Generic Spark DataFrame writer."""

    def write(self, df: DataFrame, output_def: TableDefinition, **kwargs: Any) -> None:
        """Write DataFrame to target defined in TableDefinition."""
        path = output_def.path
        file_format = output_def.format
        options = output_def.options

        if not path:
            msg = "Path must be specified in TableDefinition"
            raise ValueError(msg)
        if not file_format:
            msg = "Format must be specified in TableDefinition"
            raise ValueError(msg)

        logger.info(f"Writing {file_format} to {path}")

        writer = df.write.format(file_format)

        if options:
            writer = writer.options(**options)

        # Check for target-specific properties
        if isinstance(output_def, TargetTableDefMixin):
            if partition_by := output_def.partition_by:
                writer = writer.partitionBy(*partition_by)

            if write_mode := output_def.write_mode:
                writer = writer.mode(write_mode)

        writer.save(path)
        logger.info(f"Successfully wrote to {path}")
