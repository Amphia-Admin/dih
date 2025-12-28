"""Auto Loader reader for streaming data ingestion."""

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

from src.readers.base_spark_reader import AbstractReader

if TYPE_CHECKING:
    from src.core.table_interfaces import TableDefinition

logger = logging.getLogger(__name__)


class AutoLoaderReader(AbstractReader):
    """Reader using Databricks Auto Loader for streaming file ingestion.

    Auto Loader incrementally and efficiently processes new files as they
    arrive in cloud storage. Uses cloudFiles format under the hood.

    Common options to set in TableDefinition.options:
        - cloudFiles.format: Source file format (json, csv, parquet, etc.)
        - cloudFiles.schemaLocation: Schema inference checkpoint location
        - cloudFiles.inferColumnTypes: Infer column types (default: false)

    Note: This reader returns a streaming DataFrame. For batch processing,
    use SparkDataFrameReader or DeltaTableReader instead.
    """

    def __init__(self) -> None:
        self._data: DataFrame | None = None

    def read(self, input_def: TableDefinition) -> DataFrame:
        """Read data using Auto Loader.

        Parameters
        ----------
        input_def : TableDefinition
            Table definition with path and cloudFiles options

        Returns
        -------
        DataFrame
            The streaming DataFrame

        Raises
        ------
        RuntimeError
            If no active Spark session
        ValueError
            If path is not specified
        """
        spark = SparkSession.getActiveSession()
        if spark is None:
            msg = "No active Spark session found"
            raise RuntimeError(msg)

        path = input_def.path
        if not path:
            msg = "Path must be specified in TableDefinition for Auto Loader"
            raise ValueError(msg)

        options = input_def.options or {}
        schema = input_def.schema

        logger.info(f"Reading with Auto Loader from: {path}")
        logger.debug(f"Auto Loader options: {options}")

        reader = spark.readStream.format("cloudFiles")

        if schema:
            reader = reader.schema(schema)

        if options:
            reader = reader.options(**options)

        self._data = reader.load(path)
        return self._data

    @property
    def data(self) -> DataFrame:
        """Return the loaded DataFrame."""
        if self._data is None:
            msg = "No data loaded. Call read() first."
            raise RuntimeError(msg)
        return self._data
