
from src.dih.utils.paths import PathBuilder
from src.dih.constants import LakeLayer
from src.dih.core.table_interfaces import TableDefinition, TargetTableDefMixin
from src.dih.constants import FileFormat

class BronzeTableDefTransactions(TableDefinition, TargetTableDefMixin):
    """
    Bronze layer - raw data landing zone.

    Stores all ingested orders data with minimal transformation.
    Uses append mode to preserve complete history of all batches.
    """

    @property
    def path(self) -> str:
        """
        Build Delta table path using PathBuilder.

        Returns
        -------
        str
            Path to bronze/orders Delta table.

        Raises
        ------
        ValueError
            If root_path is not set.

        """
        if self.root_path is None:
            msg = "root_path must be set"
            raise ValueError(msg)

        return (
            # PathBuilder(self.root_path).layer(LakeLayer.BRONZE).table("orders").build()
            f"spark_catalog.{LakeLayer.BRONZE}.orders"
        )

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