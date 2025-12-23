"""Landed layer table definition for products."""

from pathlib import Path
from typing import Any

from src.dih.core.table_interfaces import TableDefinition
from src.dih.constants import FileFormat


class LandedTableDefProducts(TableDefinition):
    """
    CSV source for raw product data.

    Reads from the lake volume path.
    """

    @property
    def path(self) -> str:
        """Get path to products CSV file from lake volume."""
        lake_path = self.get_volume("lake")
        return str(Path(lake_path) / "products.csv")

    @property
    def format(self) -> str:
        """Get file format."""
        return FileFormat.CSV.value

    @property
    def options(self) -> dict[str, Any]:
        """Get CSV read options."""
        return {
            "header": "true",
            "inferSchema": "true",
        }
