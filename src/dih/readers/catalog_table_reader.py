"""Catalog table reader for reading from Unity Catalog or Hive Metastore."""

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

from src.dih.readers.base_spark_reader import AbstractReader

if TYPE_CHECKING:
    from src.dih.core.table_interfaces import TableDefinition

logger = logging.getLogger(__name__)


class CatalogTableReader(AbstractReader):
    """Reader for catalog-registered tables.

    Reads tables registered in Unity Catalog or Hive Metastore using
    fully qualified table names (catalog.schema.table or schema.table).

    This reader is format-agnostic - it reads whatever format the catalog
    table uses (Delta, Parquet, etc.).
    """

    def __init__(self) -> None:
        self._data: DataFrame | None = None

    def read(self, input_def: TableDefinition) -> DataFrame:
        """Read data from catalog table.

        Parameters
        ----------
        input_def : TableDefinition
            Table definition with table_name property

        Returns
        -------
        DataFrame
            The loaded DataFrame

        Raises
        ------
        RuntimeError
            If no active Spark session
        ValueError
            If table_name is not specified
        """
        spark = SparkSession.getActiveSession()
        if spark is None:
            msg = "No active Spark session found"
            raise RuntimeError(msg)

        table_name = getattr(input_def, "table_name", None)
        if not table_name:
            msg = "table_name must be specified in TableDefinition for CatalogTableReader"
            raise ValueError(msg)

        logger.info(f"Reading from catalog table: {table_name}")

        self._data = spark.table(table_name)
        return self._data

    @property
    def data(self) -> DataFrame:
        """Return the loaded DataFrame."""
        if self._data is None:
            msg = "No data loaded. Call read() first."
            raise RuntimeError(msg)
        return self._data
