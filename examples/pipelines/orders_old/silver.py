"""Silver layer transformation - cleaned and deduplicated data.

This module contains the transformation for cleaning, deduplicating, and
merging order data from Bronze to Silver layer. Demonstrates Delta Lake
merge operations with automatic partition locking.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.dih.readers.spark_reader import SparkDataFrameReader
from src.dih.delta.writers.merge_auto_partition import DeltaMergeAutoPartitionWriter
from src.dih.core.pipeline import (
    Pipeline,
    pipeline_definition,
)
from src.dih.core.reader_registry import register_reader
from src.dih.core.writer_registry import register_writer
from examples.pipelines.orders_old.table_definitions import BronzeOrders, SilverOrders


@pipeline_definition(name="silver_orders")
@register_reader(BronzeOrders, SparkDataFrameReader, alias="bronze_orders")
@register_writer(SilverOrders, DeltaMergeAutoPartitionWriter, alias="silver_orders")
class SilverOrdersTransformation(Pipeline):
    """
    Silver layer transformation for order data.

    Cleans and transforms bronze data, performs merge operations
    based on order_id to maintain a deduplicated view. Uses automatic
    partition locking for optimized merge performance.

    Key features:
    - Deduplication by order_id (keeps latest record)
    - Delta merge operations for upserts
    - Automatic partition detection and locking
    - Derived columns for analysis (year, month, total_amount)
    """

    def process(self) -> None:
        """
        Process bronze data to silver layer.

        Transformations applied:
        1. Deduplicate by order_id (keep latest by ingestion_timestamp)
        2. Extract year and month from order_date for partitioning
        3. Calculate total_amount (quantity * price)
        4. Merge updates based on order_id using DeltaMergeAutoPartitionWriter

        Deduplication Strategy
        ----------------------
        Uses a window function partitioned by order_id and ordered by
        ingestion_timestamp (descending) to keep only the most recent
        version of each order. This handles cases where the same order
        appears in multiple bronze batches with status updates.

        Merge Strategy
        --------------
        DeltaMergeAutoPartitionWriter automatically:
        - Detects partition values (year, month) from DataFrame
        - Locks only affected partitions for performance
        - Merges on order_id (primary key)
        - Updates existing orders, inserts new ones
        """
        # Read from bronze layer
        df = self.inputs["bronze_orders"]

        # Deduplicate: Keep latest record per order_id based on ingestion_timestamp
        # This handles cases where the same order appears in multiple bronze batches
        window_spec = Window.partitionBy("order_id").orderBy(
            F.col("ingestion_timestamp").desc()
        )
        df_deduped = (
            df.withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        # Clean and transform
        # Add derived columns for partitioning and analysis
        df_clean = (
            df_deduped.withColumn("year", F.year(F.col("order_date")))
            .withColumn("month", F.month(F.col("order_date")))
            .withColumn("total_amount", F.col("quantity") * F.col("price"))
        )

        # Write with merge (upsert based on order_id)
        # DeltaMergeAutoPartitionWriter will:
        # - Auto-detect partition values (year, month) from DataFrame
        # - Merge on order_id (primary key defined in SilverOrders)
        # - Update existing orders, insert new ones
        self.outputs.add("silver_orders", df_clean)
