from src.dih.constants import FileFormat, LakeLayer
from src.dih.core.table_interfaces import TableDefinition, TargetTableDefMixin


class BronzeTableDefTransactions(TableDefinition, TargetTableDefMixin):
    """
    Bronze layer - raw data landing zone.

    Stores all ingested orders data with minimal transformation.
    Uses append mode to preserve complete history of all batches.
    """

    @property
    def managed(self) -> bool:
        """Write to catalog as managed table."""
        return True

    @property
    def table_name(self) -> str:
        """Build fully qualified table name using injected catalog."""
        return f"{self.catalog}.{LakeLayer.BRONZE.value}.orders"

    @property
    def path(self) -> str | None:
        """Not needed for managed tables."""
        return None

    @property
    def format(self) -> str:
        """
        Get table format.

        Returns
        -------
        str
            Format identifier.

        """
        return FileFormat.DELTA.value

    @property
    def write_mode(self) -> str:
        """
        Get write mode.

        Returns
        -------
        str
            Write mode 'append' to keep all batches.

        """
        return "append"
