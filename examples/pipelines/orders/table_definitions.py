"""Table definitions for orders pipeline.

This module contains all table definitions for the orders data pipeline,
including source CSV files and target Delta tables across Bronze, Silver,
and Gold layers.
"""

from pathlib import Path
from typing import Any

from src.dih import LakeLayer, PathBuilder, TableDefinition, TargetTableDefMixin


class SourceOrdersBatch1(TableDefinition):
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

        # root_path points to examples/pipelines/orders
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
        return "csv"

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


class SourceOrdersBatch2(TableDefinition):
    """
    CSV source for raw order data - Batch 2.

    Points to the second batch of orders data containing updates
    and new orders to demonstrate merge behavior.
    """

    @property
    def path(self) -> str:
        """
        Get path to batch 2 CSV file.

        Returns
        -------
        str
            Absolute path to batch2.csv in the data directory.

        Raises
        ------
        ValueError
            If root_path is not set.

        """
        if self.root_path is None:
            msg = "root_path must be set"
            raise ValueError(msg)

        # root_path points to examples/pipelines/orders
        return str(Path(self.root_path) / "data" / "batch2.csv")

    @property
    def format(self) -> str:
        """
        Get file format.

        Returns
        -------
        str
            Format identifier 'csv'.

        """
        return "csv"

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


class BronzeOrders(TableDefinition, TargetTableDefMixin):
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
            PathBuilder(self.root_path).layer(LakeLayer.BRONZE).table("orders").build()
        )

    @property
    def format(self) -> str:
        """
        Get table format.

        Returns
        -------
        str
            Format identifier 'delta'.

        """
        return "delta"

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


class SilverOrders(TableDefinition, TargetTableDefMixin):
    """
    Silver layer - cleaned and deduplicated data.

    Maintains a single source of truth for orders with:
    - Deduplication by order_id
    - Delta merge operations for upserts
    - Partitioning by year/month for performance
    """

    @property
    def path(self) -> str:
        """
        Build Delta table path using PathBuilder.

        Returns
        -------
        str
            Path to silver/orders Delta table.

        Raises
        ------
        ValueError
            If root_path is not set.

        """
        if self.root_path is None:
            msg = "root_path must be set"
            raise ValueError(msg)

        return (
            PathBuilder(self.root_path).layer(LakeLayer.SILVER).table("orders").build()
        )

    @property
    def format(self) -> str:
        """
        Get table format.

        Returns
        -------
        str
            Format identifier 'delta'.

        """
        return "delta"

    @property
    def primary_keys(self) -> list[str]:
        """
        Get primary keys for merge operations.

        Returns
        -------
        list[str]
            Business key ['order_id'] used for merge matching.

        """
        return ["order_id"]

    @property
    def partition_by(self) -> list[str]:
        """
        Get partition columns.

        Returns
        -------
        list[str]
            Partition columns ['year', 'month'] for performance.

        """
        return ["year", "month"]


class GoldDailySales(TableDefinition, TargetTableDefMixin):
    """
    Gold layer - aggregated business metrics.

    Provides daily sales metrics including:
    - Total orders per day
    - Total revenue per day
    - Unique customers per day
    """

    @property
    def path(self) -> str:
        """
        Build Delta table path using PathBuilder.

        Returns
        -------
        str
            Path to gold/daily_sales Delta table.

        Raises
        ------
        ValueError
            If root_path is not set.

        """
        if self.root_path is None:
            msg = "root_path must be set"
            raise ValueError(msg)

        return (
            PathBuilder(self.root_path)
            .layer(LakeLayer.GOLD)
            .table("daily_sales")
            .build()
        )

    @property
    def format(self) -> str:
        """
        Get table format.

        Returns
        -------
        str
            Format identifier 'delta'.

        """
        return "delta"

    @property
    def write_mode(self) -> str:
        """
        Get write mode.

        Returns
        -------
        str
            Write mode 'overwrite' to regenerate aggregations.

        """
        return "overwrite"
