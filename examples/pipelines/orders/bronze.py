"""Bronze layer transformation - raw data ingestion.

This module contains transformations for ingesting raw CSV order data
into the Bronze layer with minimal transformation. Metadata columns
are added to track data lineage.
"""

from pyspark.sql import functions as F

from dih import (
    SparkDataFrameReader,
    SparkDataFrameWriter,
    Transformation,
    register_reader,
    register_writer,
    transformation_definition,
)
from examples.pipelines.orders.table_definitions import (
    BronzeOrders,
    SourceOrdersBatch1,
    SourceOrdersBatch2,
)


@transformation_definition(name="bronze_orders_batch1")
@register_reader(SourceOrdersBatch1, SparkDataFrameReader, alias="raw_orders")
@register_writer(BronzeOrders, SparkDataFrameWriter, alias="bronze_orders")
class BronzeOrdersBatch1Transformation(Transformation):
    """
    Bronze layer transformation for order data - Batch 1.

    Ingests raw CSV data from the first batch and adds metadata columns
    for tracking data lineage. Writes to bronze layer in append mode to
    preserve complete history.
    """

    def process(self) -> None:
        """
        Process raw order data to bronze layer.

        Transformations applied:
        - Add ingestion_timestamp column with current timestamp
        - Preserve all source columns as-is

        The data is written in append mode to maintain a complete
        audit trail of all ingested batches.
        """
        # Read raw CSV data
        df = self.inputs["raw_orders"]

        # Add metadata columns for data lineage tracking
        df_with_metadata = df.withColumn("ingestion_timestamp", F.current_timestamp())

        # Write to bronze layer (append mode)
        self.outputs.add("bronze_orders", df_with_metadata)


@transformation_definition(name="bronze_orders_batch2")
@register_reader(SourceOrdersBatch2, SparkDataFrameReader, alias="raw_orders")
@register_writer(BronzeOrders, SparkDataFrameWriter, alias="bronze_orders")
class BronzeOrdersBatch2Transformation(Transformation):
    """
    Bronze layer transformation for order data - Batch 2.

    Ingests raw CSV data from the second batch and adds metadata columns
    for tracking data lineage. Writes to bronze layer in append mode to
    preserve complete history.

    This batch contains updates to existing orders (status changes) and
    new orders, demonstrating incremental data ingestion.
    """

    def process(self) -> None:
        """
        Process raw order data to bronze layer.

        Transformations applied:
        - Add ingestion_timestamp column with current timestamp
        - Preserve all source columns as-is

        The data is written in append mode to maintain a complete
        audit trail of all ingested batches, including updates.
        """
        # Read raw CSV data
        df = self.inputs["raw_orders"]

        # Add metadata columns for data lineage tracking
        df_with_metadata = df.withColumn("ingestion_timestamp", F.current_timestamp())

        # Write to bronze layer (append mode)
        self.outputs.add("bronze_orders", df_with_metadata)
