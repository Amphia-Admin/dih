"""Delta table reader for reading from Delta format tables."""

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

from src.dih.readers.base_spark_reader import AbstractReader

if TYPE_CHECKING:
    from src.dih.core.table_interfaces import TableDefinition

logger = logging.getLogger(__name__)


class DeltaTableReader(AbstractReader):
    """Reader for Delta format tables.

    Supports reading from both path-based (unmanaged) and catalog-based (managed)
    Delta tables.

    For managed tables, set the `table_name` property on the TableDefinition.
    For unmanaged tables, set the `path` property on the TableDefinition.
    """

    def __init__(self) -> None:
        self._data: DataFrame | None = None

    def read(self, input_def: TableDefinition) -> DataFrame:
        """Read data from Delta table.

        Parameters
        ----------
        input_def : TableDefinition
            Table definition specifying either path or table_name

        Returns
        -------
        DataFrame
            The loaded DataFrame

        Raises
        ------
        RuntimeError
            If no active Spark session
        ValueError
            If neither path nor table_name is specified
        """
        spark = SparkSession.getActiveSession()
        if spark is None:
            msg = "No active Spark session found"
            raise RuntimeError(msg)

        table_name = getattr(input_def, "table_name", None)
        path = input_def.path
        options = input_def.options

        if table_name:
            logger.info(f"Reading Delta table from catalog: {table_name}")
            self._data = spark.table(table_name)
        elif path:
            logger.info(f"Reading Delta table from path: {path}")
            reader = spark.read.format("delta")
            if options:
                reader = reader.options(**options)
            self._data = reader.load(path)
        else:
            msg = "Either path or table_name must be specified in TableDefinition"
            raise ValueError(msg)

        return self._data

    @property
    def data(self) -> DataFrame:
        """Return the loaded DataFrame."""
        if self._data is None:
            msg = "No data loaded. Call read() first."
            raise RuntimeError(msg)
        return self._data
