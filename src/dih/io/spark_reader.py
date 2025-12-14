"""Spark DataFrame reader implementation."""

import logging

from pyspark.sql import DataFrame, SparkSession

from dih.core.interfaces import IReader, TableDefinition

logger = logging.getLogger(__name__)


class SparkDataFrameReader(IReader):
    """Generic Spark DataFrame reader."""

    def __init__(self) -> None:
        self._data: DataFrame | None = None

    def read(self, input_def: TableDefinition) -> DataFrame:
        """Read data from source defined in TableDefinition."""
        spark = SparkSession.getActiveSession()
        if spark is None:
            msg = "No active Spark session found"
            raise RuntimeError(msg)

        path = input_def.path
        file_format = input_def.format
        options = input_def.options
        schema = input_def.schema

        if not path:
            msg = "Path must be specified in TableDefinition"
            raise ValueError(msg)
        if not file_format:
            msg = "Format must be specified in TableDefinition"
            raise ValueError(msg)

        logger.info(f"Reading {file_format} from {path}")

        reader = spark.read.format(file_format)

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
