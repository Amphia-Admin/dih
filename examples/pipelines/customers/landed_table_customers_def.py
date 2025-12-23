"""Landed layer table definition for customers."""

from pathlib import Path
from typing import Any

from src.dih.core.table_interfaces import TableDefinition
from src.dih.constants import FileFormat


class LandedTableDefCustomers(TableDefinition):
    """
    JSON source for raw customer data.

    Reads from the lake volume path.
    """

    @property
    def path(self) -> str:
        """Get path to customers JSON file from lake volume."""
        lake_path = self.get_volume("lake")
        return str(Path(lake_path) / "raw/customers/customers.json")

    @property
    def format(self) -> str:
        """Get file format."""
        return FileFormat.JSON.value

    @property
    def options(self) -> dict[str, Any]:
        """Get JSON read options."""
        return {
            "multiLine": "true",
        }
