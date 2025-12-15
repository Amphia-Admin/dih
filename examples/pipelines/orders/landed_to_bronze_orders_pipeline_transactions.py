from pyspark.sql import functions as F

from src.dih.readers.spark_reader import SparkDataFrameReader
from src.dih.writers.spark_writer import SparkDataFrameWriter
from src.dih.core.pipeline import (
    Pipeline,
    pipeline_definition
)
from src.dih.core.reader_registry import register_reader
from src.dih.core.writer_registry import register_writer
from examples.pipelines.orders.landed_table_orders_def_transactions import LandedTableDefTransactions
from examples.pipelines.orders.bronze_table_orders_def_transactions import BronzeTableDefTransactions


@pipeline_definition(name="LandedToBronzeOrdersBatch1")
@register_reader(definition=LandedTableDefTransactions, reader=SparkDataFrameReader, alias="raw_orders")
@register_writer(definition=BronzeTableDefTransactions, writer=SparkDataFrameWriter, alias="bronze_orders")
class LandedToBronzeOrdersBatch1Transactions(Pipeline):

    def process(self) -> None:

        # Read raw CSV data
        df = self.inputs["raw_orders"]

        # Add metadata columns for data lineage tracking
        df_with_metadata = df.withColumn("ingestion_timestamp", F.current_timestamp())

        # Write to bronze layer (append mode)
        self.outputs.add("bronze_orders", df_with_metadata)
