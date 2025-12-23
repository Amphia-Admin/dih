"""Bronze layer table definition for customers."""

from src.dih.constants import FileFormat, LakeLayer
from src.dih.core.table_interfaces import TableDefinition, TargetTableDefMixin


class BronzeTableDefCustomers(TableDefinition, TargetTableDefMixin):
    """
    Bronze layer for customers data.

    Stores all ingested customer data with minimal transformation.
    Uses append mode to track customer changes over time.
    """

    @property
    def managed(self) -> bool:
        """Write to catalog as managed table."""
        return True

    @property
    def table_name(self) -> str:
        """Build fully qualified table name using injected catalog."""
        return f"{self.catalog}.{LakeLayer.BRONZE.value}.customers"

    @property
    def path(self) -> str | None:
        """Not needed for managed tables."""
        return None

    @property
    def format(self) -> str:
        """Get table format."""
        return FileFormat.DELTA.value

    @property
    def write_mode(self) -> str:
        """Get write mode - append to track changes."""
        return "append"
