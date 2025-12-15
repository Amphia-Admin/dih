"""Base Delta writer with shared logic for merge operations."""

import logging
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from delta.tables import DeltaTable

from src.dih.core.table_interfaces import TableDefinition, TargetTableDefMixin
from src.dih.writers.base_spark_writer import AbstractWriter
if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DeltaWriterBase(AbstractWriter):
    """Abstract base class for Delta writers with merge capabilities."""

    def write(self, df: DataFrame, output_def: TableDefinition, **kwargs: Any) -> None:
        """
        Write DataFrame to Delta table with merge or initial write.

        Routes to merge operation if table exists, otherwise creates new table.

        Parameters
        ----------
        df : DataFrame
            DataFrame to write
        output_def : TableDefinition
            Target table definition
        **kwargs : Any
            Additional write options
        """
        path = output_def.path
        if path is None:
            msg = "Output definition must have a path specified"
            raise ValueError(msg)

        logger.info(f"Writing to Delta table at: {path}")

        # Get Spark session from DataFrame
        spark = df.sparkSession

        # Check if Delta table already exists
        if DeltaTable.isDeltaTable(spark, path):
            logger.info("Delta table exists - performing merge operation")
            self._merge(df, output_def, spark, path, **kwargs)
        else:
            logger.info("Delta table does not exist - performing initial write")
            self._initial_write(df, output_def, path, **kwargs)

        logger.info("Write operation completed successfully")

    @abstractmethod
    def _merge(
        self,
        df: DataFrame,
        output_def: TableDefinition,
        spark: SparkSession,
        path: str,
        **kwargs: Any,
    ) -> None:
        """
        Perform merge operation on existing Delta table.

        Must be implemented by subclasses to define merge strategy.

        Parameters
        ----------
        df : DataFrame
            Source DataFrame to merge
        output_def : TableDefinition
            Target table definition
        spark : SparkSession
            SparkSession instance
        path : str
            Path to Delta table
        **kwargs : Any
            Additional merge options
        """
        ...

    def _initial_write(
        self,
        df: DataFrame,
        output_def: TableDefinition,
        path: str,
        **kwargs: Any,
    ) -> None:
        """
        Create new Delta table with initial data.

        Parameters
        ----------
        df : DataFrame
            DataFrame to write
        output_def : TableDefinition
            Target table definition
        path : str
            Path to create Delta table at
        **kwargs : Any
            Additional write options
        """
        logger.info(f"Creating new Delta table at: {path}")

        writer = df.write.format("delta")

        # Apply partition columns if specified
        if isinstance(output_def, TargetTableDefMixin) and (
            partition_by := output_def.partition_by
        ):
            logger.info(f"Partitioning by: {partition_by}")
            writer = writer.partitionBy(*partition_by)

        # Apply any custom options from output_def
        if options := output_def.options:
            logger.debug(f"Applying write options: {options}")
            writer = writer.options(**options)

        # Write as new Delta table
        writer.save(path)
        logger.info("Initial Delta table created successfully")
