"""Bronze layer table definition for products."""

from src.dih.constants import FileFormat, LakeLayer
from src.dih.core.table_interfaces import TableDefinition, TargetTableDefMixin


class BronzeTableDefProducts(TableDefinition, TargetTableDefMixin):
    """
    Bronze layer for products data.

    Stores all ingested product data with minimal transformation.
    Uses append mode for incremental updates.
    """

    @property
    def managed(self) -> bool:
        """Write to catalog as managed table."""
        return True

    @property
    def table_name(self) -> str:
        """Build fully qualified table name using injected catalog."""
        return f"{LakeLayer.BRONZE.value}.products"

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
        """Get write mode - append for incremental updates."""
        return "append"
