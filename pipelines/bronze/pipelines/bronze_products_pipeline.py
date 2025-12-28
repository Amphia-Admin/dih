"""Bronze pipeline for products - ingests from landed layer."""

from pyspark.sql import functions as F

from src.core.pipeline import Pipeline, pipeline_definition
from src.core.reader_registry import register_reader
from src.core.writer_registry import register_writer
from src.readers.base_spark_reader import SparkDataFrameReader
from src.writers.base_spark_writer import SparkDataFrameWriter
from src.transforms.apply_schema import apply_schema

from pipelines.landed.definitions.landed_products_def import LandedProductsDef
from pipelines.bronze.definitions.bronze_products_def import BronzeProductsDef
from pipelines.bronze.schemas.bronze_products_schema import BRONZE_PRODUCTS_SCHEMA


@pipeline_definition(name="BronzeProductsPipeline")
@register_reader(definition=LandedProductsDef, reader=SparkDataFrameReader, alias="landed_products")
@register_writer(definition=BronzeProductsDef, writer=SparkDataFrameWriter, alias="bronze_products")
class BronzeProductsPipeline(Pipeline):
    """
    Ingest raw product data from landed CSV to Bronze layer.

    Adds metadata columns for data lineage tracking.
    """

    def process(self) -> None:
        df = self.inputs["landed_products"]

        df_with_metadata = (
            df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        df_final = apply_schema(df_with_metadata, BRONZE_PRODUCTS_SCHEMA)
        self.outputs.add("bronze_products", df_final)
