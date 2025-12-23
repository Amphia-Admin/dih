from pathlib import Path
from typing import Any

from src.dih.core.table_interfaces import TableDefinition
from src.dih.constants import FileFormat


class LandedTableDefTransactions(TableDefinition):
    """
    CSV source for raw order data - Batch 1.

    Reads from the lake volume path.
    """

    @property
    def path(self) -> str:
        """Get path to batch 1 CSV file from lake volume."""
        lake_path = self.get_volume("lake")
        return str(Path(lake_path) / "raw/orders/batch1.csv")

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