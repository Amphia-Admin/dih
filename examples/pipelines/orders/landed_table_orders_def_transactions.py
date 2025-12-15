from pathlib import Path
from typing import Any

from src.dih.core.table_interfaces import TableDefinition
from src.dih.constants import FileFormat

class LandedTableDefTransactions(TableDefinition):
    """
    CSV source for raw order data - Batch 1.

    Points to the first batch of orders data for initial load.
    """

    @property
    def path(self) -> str:
        """
        Get path to batch 1 CSV file.

        Returns
        -------
        str
            Absolute path to batch1.csv in the data directory.

        Raises
        ------
        ValueError
            If root_path is not set.

        """
        if self.root_path is None:
            msg = "root_path must be set"
            raise ValueError(msg)

        return str(Path(self.root_path) / "data" / "batch1.csv")

    @property
    def format(self) -> str:
        """
        Get file format.

        Returns
        -------
        str
            Format identifier 'csv'.

        """
        return FileFormat.CSV.value

    @property
    def options(self) -> dict[str, Any]:
        """
        Get CSV read options.

        Returns
        -------
        dict[str, Any]
            Spark CSV reader options with header and schema inference.

        """
        return {
            "header": "true",
            "inferSchema": "true",
        }