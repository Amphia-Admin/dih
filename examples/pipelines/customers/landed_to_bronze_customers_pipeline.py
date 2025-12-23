"""Landed to Bronze pipeline for customers."""

from pyspark.sql import functions as F

from src.dih.core.pipeline import Pipeline, pipeline_definition
from src.dih.core.reader_registry import register_reader
from src.dih.core.writer_registry import register_writer
from src.dih.readers.base_spark_reader import SparkDataFrameReader
from src.dih.writers.base_spark_writer import SparkDataFrameWriter

from examples.pipelines.customers.landed_table_customers_def import LandedTableDefCustomers
from examples.pipelines.customers.bronze_table_customers_def import BronzeTableDefCustomers


@pipeline_definition(name="LandedToBronzeCustomers")
@register_reader(definition=LandedTableDefCustomers, reader=SparkDataFrameReader, alias="raw_customers")
@register_writer(definition=BronzeTableDefCustomers, writer=SparkDataFrameWriter, alias="bronze_customers")
class LandedToBronzeCustomersPipeline(Pipeline):
    """
    Ingest raw customer data from JSON to Bronze layer.

    Adds metadata columns for data lineage tracking.
    """

    def process(self) -> None:
        # Read raw JSON data
        df = self.inputs["raw_customers"]

        # Add metadata columns for data lineage tracking
        df_with_metadata = (
            df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        # Write to bronze layer
        self.outputs.add("bronze_customers", df_with_metadata)
