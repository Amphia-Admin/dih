"""Schema for bronze products data."""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=True),
    StructField("price", DecimalType(10, 2), nullable=True),
    StructField("stock_quantity", IntegerType(), nullable=True),
    StructField("supplier_id", StringType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("source_file", StringType(), nullable=False),
])
