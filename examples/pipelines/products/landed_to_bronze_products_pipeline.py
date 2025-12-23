"""Landed to Bronze pipeline for products."""

from pyspark.sql import functions as F

from src.dih.core.pipeline import Pipeline, pipeline_definition
from src.dih.core.reader_registry import register_reader
from src.dih.core.writer_registry import register_writer
from src.dih.readers.base_spark_reader import SparkDataFrameReader
from src.dih.writers.base_spark_writer import SparkDataFrameWriter

from examples.pipelines.products.landed_table_products_def import LandedTableDefProducts
from examples.pipelines.products.bronze_table_products_def import BronzeTableDefProducts


@pipeline_definition(name="LandedToBronzeProducts")
@register_reader(definition=LandedTableDefProducts, reader=SparkDataFrameReader, alias="raw_products")
@register_writer(definition=BronzeTableDefProducts, writer=SparkDataFrameWriter, alias="bronze_products")
class LandedToBronzeProductsPipeline(Pipeline):
    """
    Ingest raw product data from CSV to Bronze layer.

    Adds metadata columns for data lineage tracking.
    """

    def process(self) -> None:
        # Read raw CSV data
        df = self.inputs["raw_products"]

        # Add metadata columns for data lineage tracking
        df_with_metadata = (
            df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        # Write to bronze layer
        self.outputs.add("bronze_products", df_with_metadata)
